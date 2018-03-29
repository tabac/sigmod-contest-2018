#include <iostream>
#include <algorithm>
#include <queue>
#include "Planner.hpp"
#include "DataEngine.hpp"
#include "Plan.hpp"
#include "Parser.hpp"
#include "Relation.hpp"
#include "Utils.hpp"

#define FILTER_SELECT_THRES 0.8
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
int intermDataCounter = 0;
int targetCounter = 0;
//---------------------------------------------------------------------------
bool Planner::filterComparator(const FilterInfo& f1, const FilterInfo& f2)
{
    return DataEngine::getFilterSelectivity(f1) <= DataEngine::getFilterSelectivity(f2);
}
//---------------------------------------------------------------------------
bool Planner::predicateComparator(const PredicateInfo& p1, const PredicateInfo& p2)
{
//    cout << "COMPARING " << DataEngine::getJoinSelectivity(p1) << " with " << DataEngine::getJoinSelectivity(p2) << endl;
//    cout << "COMPARING " << DataEngine::getJoinEstimatedTuples(p1) << " with " << DataEngine::getJoinEstimatedTuples(p2) << endl;
//    return DataEngine::getJoinSelectivity(p1) < DataEngine::getJoinSelectivity(p2);
    return DataEngine::getJoinEstimatedTuples(p1) < DataEngine::getJoinEstimatedTuples(p2);
}
//---------------------------------------------------------------------------
void Planner::updateAttached(OriginTracker &lastAttached, const unsignedPair relationPair, AbstractNode *newNode)
{
    // Fail, total fail.
    AbstractNode *oldNode = lastAttached[relationPair];

    if (!oldNode->isBaseRelation()) {
        unordered_map<unsignedPair, AbstractNode *>::iterator jt;
        for (jt = lastAttached.begin(); jt != lastAttached.end(); ++jt) {
            if (jt->second == oldNode) {
                jt->second = newNode;
            }
        }
    } else {
        lastAttached[relationPair] = newNode;
    }
}
//---------------------------------------------------------------------------
static void recursivePropagateSelection(QueryInfo &query,AbstractOperatorNode *o,
                               const SelectInfo &selection, unsigned count)
{
    assert(o->hasBinding(selection.binding));
    queue<AbstractOperatorNode *> q;

    q.push(o);

    if (Utils::contains(query.selections, selection)) {
        while(!q.empty()) {
            AbstractOperatorNode *cur = q.front();
            q.pop();

            if (!cur->outAdjList[0]->outAdjList.empty()) {

                cur->selections.emplace_back(selection);

                vector<AbstractNode *>::iterator jt;
                for (jt = cur->outAdjList[0]->outAdjList.begin(); jt != cur->outAdjList[0]->outAdjList.end(); ++jt) {
                    AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);
                    q.push(o);
//                    if (o->hasSelection(selection)) {
//                        q.push(o);
//                    }
                }
            }
        }
    } else {
        while(!q.empty()) {
            AbstractOperatorNode *cur = q.front();
            q.pop();

            if (!cur->outAdjList[0]->outAdjList.empty()) {
                if (cur->hasSelection(selection)) {
                    --count;
                }

                if (count != 0) {
                    cur->selections.emplace_back(selection);

                    vector<AbstractNode *>::iterator jt;
                    for (jt = cur->outAdjList[0]->outAdjList.begin(); jt != cur->outAdjList[0]->outAdjList.end(); ++jt) {
                        AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);
                        q.push(o);
//                        if (o->hasSelection(selection)) {
//                            q.push(o);
//                        }
                    }
                } else {
                    break;
                }
            }
        }
    }
}
//---------------------------------------------------------------------------
static void propagateSelection(QueryInfo &query, AbstractOperatorNode *o,
                               const SelectInfo &selection, unsigned count)
{
    assert(o->hasBinding(selection.binding));
    if (Utils::contains(query.selections, selection)) {
        while (!o->outAdjList[0]->outAdjList.empty()) {
            o->selections.emplace_back(selection);

            o = (AbstractOperatorNode *) o->outAdjList[0]->outAdjList[0];

        }
    } else {
        while (!o->outAdjList[0]->outAdjList.empty()) {
            if (o->hasSelection(selection)) {
                --count;
            }

            if (count != 0) {
                o->selections.emplace_back(selection);

                o = (AbstractOperatorNode *) o->outAdjList[0]->outAdjList[0];
            } else {
                break;
            }
        }
    }
}
//---------------------------------------------------------------------------
static Relation *findRelationBySelection(Plan &plan, const SelectInfo &selection)
{
    vector<AbstractNode *>::const_iterator it;
    for (it = plan.root->outAdjList.begin(); it != plan.root->outAdjList.end(); ++it) {
        Relation *r = (Relation *) (*it);

        if (r->relId == selection.relId) {
            return r;
        }
    }

    assert(false);

    return NULL;
}
//---------------------------------------------------------------------------
void Planner::setQuerySelections(Plan &plan, QueryInfo &query)
{
    unordered_map<SelectInfo, unsigned> selectionsMap;

    query.getSelectionsMap(selectionsMap);

    unordered_map<SelectInfo, unsigned>::const_iterator it;
    for (it = selectionsMap.begin(); it != selectionsMap.end(); ++it) {
        Relation *r = (Relation *) findRelationBySelection(plan, it->first);

        vector<AbstractNode *>::iterator jt;
        for (jt = r->outAdjList.begin(); jt != r->outAdjList.end(); ++jt) {
            AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);

            if (o->hasBinding(it->first.binding)) {
                //propagateSelection(query, o, it->first, it->second);
                recursivePropagateSelection(query,o, it->first, it->second);
            }
        }
    }
}
//---------------------------------------------------------------------------
//void Planner::setQuerySelections(Plan &plan, unordered_map<SelectInfo, unsigned> &selectionsMap)
//{
//    unordered_map<SelectInfo, unsigned>::const_iterator it;
//    for (it = selectionsMap.begin(); it != selectionsMap.end(); ++it) {
//        Relation *r = (Relation *) findRelationBySelection(plan, it->first);
//
//        vector<AbstractNode *>::iterator jt;
//        for (jt = r->outAdjList.begin(); jt != r->outAdjList.end(); ++jt) {
//            AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);
//
//            if (o->hasBinding(it->first.binding)) {
//                //propagateSelection(query, o, it->first, it->second);
//                recursivePropagateSelection(query,o, it->first, it->second);
//            }
//        }
//    }
//}
//---------------------------------------------------------------------------

unsigned Planner::addFilters(Plan &plan, QueryInfo& query, OriginTracker &lastAttached)
{

    unsigned counter = 0;
    vector<FilterInfo>::iterator ft;
    for(ft = query.filters.begin(); ft != query.filters.end(); ++ft){
        float selectivity;
        if ((selectivity = DataEngine::getFilterSelectivity(*ft)) <= FILTER_SELECT_THRES){
#ifndef NDEBUG
            cout << "FILTER SELECTIVITY " << selectivity << endl;
#endif
            DataNode *dataNode = new DataNode();
            FilterOperatorNode *filterNode = new FilterOperatorNode((*ft));

            unsignedPair filterPair = {ft->filterColumn.relId,
                                       ft->filterColumn.binding};

            AbstractNode::connectNodes(lastAttached[filterPair], filterNode);
            AbstractNode::connectNodes(filterNode, dataNode);

            Planner::updateAttached(lastAttached, filterPair, dataNode);

            plan.nodes.push_back((AbstractNode *) filterNode);
            plan.nodes.push_back((AbstractNode *) dataNode);
            counter++;

#ifndef NDEBUG
            filterNode->label = filterNode->info.dumpLabel();
            dataNode->label = "d" + to_string(intermDataCounter++);
#endif
        }else{
            break;
        }
    }
#ifndef NDEBUG
    cout << "Start from counter " << counter << endl;
#endif

    return counter;
}
//---------------------------------------------------------------------------
void Planner::addFilters2(Plan &plan, QueryInfo& query, OriginTracker &lastAttached)
{

    unsigned counter = 0;
    vector<FilterInfo>::iterator ft;
    for(ft = query.filters.begin(); ft != query.filters.end(); ++ft){
            DataNode *dataNode = new DataNode();
            FilterOperatorNode *filterNode = new FilterOperatorNode((*ft));

            unsignedPair filterPair = {ft->filterColumn.relId,
                                       ft->filterColumn.binding};

            AbstractNode::connectNodes(lastAttached[filterPair], filterNode);
            AbstractNode::connectNodes(filterNode, dataNode);

            Planner::updateAttached(lastAttached, filterPair, dataNode);

            plan.nodes.push_back((AbstractNode *) filterNode);
            plan.nodes.push_back((AbstractNode *) dataNode);
            counter++;

#ifndef NDEBUG
            filterNode->label = filterNode->info.dumpLabel();
            dataNode->label = "d" + to_string(intermDataCounter++);
#endif

    }
#ifndef NDEBUG
    cout << "Start from counter " << counter << endl;
#endif
}

//---------------------------------------------------------------------------
void Planner::addRemainingFilters(Plan &plan, QueryInfo& query, unsigned pft, OriginTracker &lastAttached)
{

    vector<FilterInfo>::iterator ft;
    for(ft = query.filters.begin()+pft; ft != query.filters.end(); ++ft){

        DataNode *dataNode = new DataNode();
        FilterOperatorNode *filterNode = new FilterOperatorNode((*ft));

        unsignedPair filterPair = {ft->filterColumn.relId,
                                   ft->filterColumn.binding};

        AbstractNode::connectNodes(lastAttached[filterPair], filterNode);
        AbstractNode::connectNodes(filterNode, dataNode);

        Planner::updateAttached(lastAttached, filterPair, dataNode);

        plan.nodes.push_back((AbstractNode *) filterNode);
        plan.nodes.push_back((AbstractNode *) dataNode);

#ifndef NDEBUG
            filterNode->label = filterNode->info.dumpLabel();
            dataNode->label = "d" + to_string(intermDataCounter++);
#endif

    }
}
//---------------------------------------------------------------------------
void Planner::addJoins(Plan& plan, QueryInfo& query, OriginTracker &lastAttached)
{

    vector<PredicateInfo>::iterator pt, qt;
    for(pt = query.predicates.begin(); pt != query.predicates.end(); ++pt) {
        // Check if the symmetric predicate is already added.
        // If so skip current predicate
        bool skip_predicate = false;
        for (qt = query.predicates.begin(); qt != pt; ++qt) {
            if ((*qt).left == (*pt).right && (*qt).right == (*pt).left) {
                skip_predicate = true;
            }
        }

        if (!skip_predicate) {
            if ((*pt).left.relId == (*pt).right.relId &&
                (*pt).left.binding == (*pt).right.binding) {
                // If predicate refers to the same table
                // add `FilterJoinOperatorNode`.
                Planner::addFilterJoin(plan, (*pt), query, lastAttached);
            } else {
                unsignedPair leftPair = {(*pt).left.relId,
                                         (*pt).left.binding};
                unsignedPair rightPair = {(*pt).right.relId,
                                          (*pt).right.binding};

                if (lastAttached[leftPair] == lastAttached[rightPair]) {
                    Planner::addFilterJoin(plan, (*pt), query, lastAttached);
                } else {
                    // If predicate refers to different tables
                    // add `JoinOperatorNode`.
                    Planner::addJoin(plan, (*pt), query, lastAttached);
                }
            }
        }
    }
}
//---------------------------------------------------------------------------
void Planner::addNonSharedJoins(Plan& plan, QueryInfo& query, OriginTracker &lastAttached)
{

    vector<PredicateInfo>::iterator pt, qt;
    for(pt = query.predicates.begin(); pt != query.predicates.end(); ++pt) {

        cout << "AVAILABLE SHARES" << endl;
        for(JoinCatalog::iterator st = plan.sharedJoins.begin();st != plan.sharedJoins.end(); st++){
            cout << (st->first).dumpLabel() << endl;
        }

        if(plan.sharedJoins.find(*pt)!=plan.sharedJoins.end()){
            cout << "SKIP SHAREEEd " << (*pt).dumpLabel() << endl;
            continue;
        }
        //cout << "NOT SH " << (*pt).dumpLabel() << endl;

        // Check if the symmetric predicate is already added.
        // If so skip current predicate
        bool skip_predicate = false;
        for (qt = query.predicates.begin(); qt != pt; ++qt) {
            if ((*qt).left == (*pt).right && (*qt).right == (*pt).left) {
                skip_predicate = true;
            }
        }

        if (!skip_predicate) {
            if ((*pt).left.relId == (*pt).right.relId &&
                (*pt).left.binding == (*pt).right.binding) {
                // If predicate refers to the same table
                // add `FilterJoinOperatorNode`.
                Planner::addFilterJoin(plan, (*pt), query, lastAttached);
            } else {
                unsignedPair leftPair = {(*pt).left.relId,
                                         (*pt).left.binding};
                unsignedPair rightPair = {(*pt).right.relId,
                                          (*pt).right.binding};

                if (lastAttached[leftPair] == lastAttached[rightPair]) {
                    Planner::addFilterJoin(plan, (*pt), query, lastAttached);
                } else {
                    // If predicate refers to different tables
                    // add `JoinOperatorNode`.
                    Planner::addJoin(plan, (*pt), query, lastAttached);
                }
            }
        }
    }
}
//---------------------------------------------------------------------------
void Planner::addJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query, OriginTracker &lastAttached)
{
    DataNode *dataNode = new DataNode();
    JoinOperatorNode *joinNode = new JoinOperatorNode(predicate);


    unsignedPair leftPair = {predicate.left.relId,
                             predicate.left.binding};
    unsignedPair rightPair = {predicate.right.relId,
                              predicate.right.binding};

    AbstractNode::connectNodes(lastAttached[leftPair], joinNode);
    AbstractNode::connectNodes(lastAttached[rightPair], joinNode);
    AbstractNode::connectNodes(joinNode, dataNode);

    Planner::updateAttached(lastAttached, leftPair, dataNode);
    Planner::updateAttached(lastAttached, rightPair, dataNode);

    plan.nodes.push_back((AbstractNode *) joinNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

#ifndef NDEBUG
    joinNode->label = predicate.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
#endif

}
//---------------------------------------------------------------------------
void Planner::addSharedJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query, OriginTracker &lastAttached)
{

    JoinOperatorNode* joinNode;
    DataNode *dataNode;

    unsignedPair leftPair = {predicate.left.relId,
                             predicate.left.binding};
    unsignedPair rightPair = {predicate.right.relId,
                              predicate.right.binding};

    try {
        joinNode = plan.sharedJoins.at(predicate);
        auto nhandler = plan.sharedJoins.extract(predicate);
        auto predicateKey = nhandler.key();
        plan.sharedJoins.insert(move(nhandler));
        joinNode->updateBindings(predicateKey);
        dataNode = (DataNode*) joinNode->outAdjList[0];
        cout << "Join Result issss " << dataNode->label << endl;

//        Planner::updateAttached(lastAttached, leftPair, dataNode);
//        Planner::updateAttached(lastAttached, rightPair, dataNode);
    }
    catch (const out_of_range &) {
        joinNode = new JoinOperatorNode(predicate);
        dataNode = new DataNode();

        #ifndef NDEBUG
            joinNode->label = predicate.dumpLabel();
            dataNode->label = "d" + to_string(intermDataCounter++);
        #endif

        cout << "Persist join node with predicate " << predicate.dumpLabel() << endl;
        cout << "Join Result issss " << dataNode->label << endl;
        cout << "CONNECT " << (joinNode->info).dumpLabel() << " to " << lastAttached[leftPair]->label << " and " << lastAttached[rightPair]->label << endl;

        AbstractNode::connectNodes(lastAttached[leftPair], joinNode);
        AbstractNode::connectNodes(lastAttached[rightPair], joinNode);
        AbstractNode::connectNodes(joinNode, dataNode);

//        Planner::updateAttached(lastAttached, leftPair, dataNode);
//        Planner::updateAttached(lastAttached, rightPair, dataNode);

        plan.nodes.push_back((AbstractNode *) joinNode);
        plan.nodes.push_back((AbstractNode *) dataNode);

        plan.sharedJoins[predicate] = joinNode;
    }

    Planner::updateAttached(lastAttached, leftPair, dataNode);
    Planner::updateAttached(lastAttached, rightPair, dataNode);

    cout << "MALAKIEEEEES" << endl;
    printPlan(&plan);
    cout << endl;


}
//---------------------------------------------------------------------------
void Planner::addFilterJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query, OriginTracker &lastAttached)
{
    DataNode *dataNode = new DataNode();
    FilterJoinOperatorNode *joinNode = new FilterJoinOperatorNode(predicate);

    unsignedPair leftPair = {predicate.left.relId,
                             predicate.left.binding};
    unsignedPair rightPair = {predicate.right.relId,
                              predicate.right.binding};

    assert(leftPair == rightPair ||
           lastAttached[leftPair] == lastAttached[rightPair]);

    AbstractNode::connectNodes(lastAttached[leftPair], joinNode);
    AbstractNode::connectNodes(joinNode, dataNode);

    Planner::updateAttached(lastAttached, leftPair, dataNode);

    plan.nodes.push_back((AbstractNode *) joinNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

#ifndef NDEBUG
    joinNode->label = joinNode->info.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
#endif

}
//---------------------------------------------------------------------------
void Planner::addAggregate(Plan &plan, const QueryInfo& query, OriginTracker &lastAttached)
{
    DataNode *dataNode =  new DataNode();
    AggregateOperatorNode *aggregateNode = new AggregateOperatorNode();

    aggregateNode->queryId = query.queryId;

    aggregateNode->selections = query.selections;

    AbstractNode::connectNodes((AbstractNode *) aggregateNode,
                               (AbstractNode *) dataNode);

    unordered_map<unsignedPair, AbstractNode *>::iterator it;
    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
        if ((*it).second->outAdjList.empty()) {
            AbstractNode::connectNodes((AbstractNode *) (*it).second,
                                       (AbstractNode *) aggregateNode);
        }
    }

    plan.nodes.push_back((AbstractNode *) aggregateNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

    plan.exitNodes.push_back(dataNode);

#ifndef NDEBUG
    // auto to assertion esteke oso to lastAttach htan per query

    AbstractNode *anode = (*lastAttached.begin()).second;
    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
        assert(anode == (*it).second);
    }
    aggregateNode->label = "aggr";
    dataNode->label = "t" + to_string(targetCounter++);
#endif

}
//---------------------------------------------------------------------------
OriginTracker Planner::connectQueryBaseRelations(Plan &plan, QueryInfo &query)
{
    OriginTracker lastAttached;
    // Push original relations.
    unsigned bd;
    vector<RelationId>::const_iterator rt;
    for (bd = 0, rt = query.relationIds.begin(); rt != query.relationIds.end(); ++rt, ++bd) {

        vector<AbstractNode *>::iterator lt;
        lt = find(plan.nodes.begin(), plan.nodes.end(), &DataEngine::relations[(*rt)]);

        if (lt == plan.nodes.end()) {
            plan.nodes.push_back((AbstractNode *) &DataEngine::relations[(*rt)]);

            AbstractNode::connectNodes(plan.root, plan.nodes.back());
            lastAttached[make_pair((*rt), bd)] = plan.nodes.back();
        } else {
            lastAttached[make_pair((*rt), bd)] = (*lt);
        }
    }
    return lastAttached;
}
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
OriginTracker Planner::connectQueryBaseRelations(Plan &plan, QueryInfo &query, OriginTracker& lastAttached)
{
    // Push original relations.
    unsigned bd;
    vector<RelationId>::const_iterator rt;
    for (bd = 0, rt = query.relationIds.begin(); rt != query.relationIds.end(); ++rt, ++bd) {

        vector<AbstractNode *>::iterator lt;
        lt = find(plan.nodes.begin(), plan.nodes.end(), &DataEngine::relations[(*rt)]);

        if (lt == plan.nodes.end()) {
            plan.nodes.push_back((AbstractNode *) &DataEngine::relations[(*rt)]);

            AbstractNode::connectNodes(plan.root, plan.nodes.back());
            lastAttached[make_pair((*rt), bd)] = plan.nodes.back();
        } else {
            lastAttached[make_pair((*rt), bd)] = (*lt);
        }
    }
    return lastAttached;
}
//---------------------------------------------------------------------------

void Planner::attachQueryPlan(Plan &plan, QueryInfo &query)
{

    OriginTracker lastAttached = Planner::connectQueryBaseRelations(plan, query);


    // sort filters by selectivity order.
    //sort(query.filters.begin(), query.filters.end(), filterComparator);

#ifndef NDEBUG
    cout << "Add all high selectivity filters." << endl;
#endif

    // Push selective filters.
    //unsigned remainingFiltersIndex = Planner::addFilters(plan, query, lastAttached);
    Planner::addFilters2(plan, query, lastAttached);

    sort(query.predicates.begin(), query.predicates.end(), predicateComparator);

#ifndef NDEBUG
    cout << "Add all joins." << endl;
#endif

    // Push join predicates.
    Planner::addJoins(plan, query, lastAttached);

#ifndef NDEBUG
    //cout << "# of filters: " << query.filters.size() <<". Pushed: " << remainingFiltersIndex << endl;
    cout << "Add all low selectivity filters." << endl;
#endif

//    if(remainingFiltersIndex < query.filters.size()){
//        // add remaining filters
//        addRemainingFilters(plan, query, remainingFiltersIndex, lastAttached);
//    }

#ifndef NDEBUG
    cout << "Add aggregates" << endl;
#endif

    // Push aggregate.
    Planner::addAggregate(plan, query, lastAttached);

    // Setup selections for OperatorNodes.
    setQuerySelections(plan, query);
}
//---------------------------------------------------------------------------
void Planner::attachQueryPlanShared(Plan &plan, QueryInfo &query, OriginTracker& lastAttached)
{

//    OriginTracker lastAttached = Planner::connectQueryBaseRelations(plan, query);
//
//    if(!plan.cJoin.empty())
//    {
//        // check if this query contains one of the shared joins and push it first
//        for (vector<PredicateInfo>::iterator pt = query.predicates.begin(); pt != query.predicates.end(); pt++) {
////        if(plan.commonJoins.find(*pt)!=plan.commonJoins.end() && plan.commonJoins.at(*pt) > 1){
////            cout << "SHARED JOIN FOUND" << endl;
////           Planner::addSharedJoin(plan, *pt, query, lastAttached);
////        }
//
//            if (*pt == plan.cJoin.back()) {
//                cout << "ADD the Shared " << (*pt).dumpLabel() << endl;
//                Planner::addSharedJoin(plan, *pt, query, lastAttached);
//            }
//        }
//    }

    cout << "NEW QUERY " << endl;

    //cout << "FINITO WITH SHARED JOINS" << endl;
    // push filters
    Planner::addFilters2(plan, query, lastAttached);

    cout << "ADD filters " << endl;
    printPlan(&plan);
    cout << endl;

    //cout << "FINITO WITH FILTERS" << endl;
    // push remaining joins in order
    //sort(query.predicates.begin(), query.predicates.end(), predicateComparator);

    cout << "ADD REST JOINS " << endl;
    printPlan(&plan);
    cout << endl;

    // Push join predicates.
    Planner::addNonSharedJoins(plan, query, lastAttached);

    cout << "ADD AGGREGATES " << endl;
    printPlan(&plan);
    cout << endl;

    //cout << "FINITO WITH NON SHARED JOINS" << endl;
    // Push aggregate.
    Planner::addAggregate(plan, query, lastAttached);

    //cout << "FINITO WITH AGGREGATES" << endl;
    // Setup selections for OperatorNodes.
    //cout << "ADDING SELECTIONS" << endl;
    //setQuerySelections(plan, query);
    cout << endl;
    printPlan(&plan);
    cout << endl;
}
//---------------------------------------------------------------------------
CommonJoinCounter Planner::findCommonJoins(vector<QueryInfo> &batch)
{
    CommonJoinCounter commonJoins;
    vector<QueryInfo>::iterator it;
    for(it = batch.begin(); it != batch.end(); ++it) {
        for(vector<PredicateInfo>::iterator pt = (*it).predicates.begin(); pt != (*it).predicates.end(); pt++){
            // if it is a pure join and not a filter join
            if (!((*pt).left.relId == (*pt).right.relId && (*pt).left.binding == (*pt).right.binding)){
                try {
                    commonJoins.at(*pt) += 1;
                }
                catch (const out_of_range &) {
                    commonJoins[*pt] = 1;
                }
            }
        }
    }
    return commonJoins;
};
//---------------------------------------------------------------------------
Plan* Planner::generatePlan(vector<QueryInfo> &queries)
{
    Plan *plan = new Plan();
    AbstractNode* root = new DataNode();

    plan->nodes.push_back(root);
    plan->root = root;

    plan->commonJoins = Planner::findCommonJoins(queries);


    //=========================================
    for(CommonJoinCounter::iterator cj = plan->commonJoins.begin(); cj != plan->commonJoins.end(); cj++){
        if(cj->second > 1){
            cout << "SHARED JOIN FOUND" << endl;
            (plan->cJoin).push_back(cj->first);
            break;
        }
    }
    //==========================================

    OriginTracker lastAttached;
    // first add shared joins
    vector<QueryInfo>::iterator it;
    for(it = queries.begin(); it != queries.end(); ++it) {
        connectQueryBaseRelations(*plan, *it, lastAttached);
        if(!(plan->cJoin).empty())
        {
            // check if this query contains one of the shared joins and push it first
            for (vector<PredicateInfo>::iterator pt = (*it).predicates.begin(); pt != (*it).predicates.end(); pt++) {
                if (*pt == (plan->cJoin).back()) {
                    cout << "ADD the Shared " << (*pt).dumpLabel() << endl;
                    Planner::addSharedJoin(*plan, *pt, (*it), lastAttached);
                }
            }
        }

    }


    //unordered_map<SelectInfo, unsigned> selectionsMap;
    for(it = queries.begin(); it != queries.end(); ++it) {
        Planner::attachQueryPlanShared(*plan, (*it), lastAttached);
        //(*it).getSelectionsMap(selectionsMap);
    }

    for(it = queries.begin(); it != queries.end(); ++it) {
        setQuerySelections(*plan, *it);
    }


#ifndef NDEBUG
    root->label = "global_root";
    vector<AbstractNode *>::iterator jt;
    for (jt = plan->nodes.begin(); jt != plan->nodes.end(); ++jt) {
        assert((*jt)->status ==  NodeStatus::fresh);
        assert((*jt)->visited == 0);
    }

    printPlan(plan);
#endif

    return plan;
}
//---------------------------------------------------------------------------
#ifndef NDEBUG
void Planner::printPlan(Plan* plan)
{
    vector<AbstractNode*>::iterator node;
    for(node = plan->nodes.begin(); node != plan->nodes.end(); node++){
        string children = "";
        vector<AbstractNode*>::iterator ch;
        for(ch = (*node)->outAdjList.begin(); ch != (*node)->outAdjList.end(); ch++){
           children += (*ch)->label+" ";
        }
        string parents = "";
        for(ch = (*node)->inAdjList.begin(); ch != (*node)->inAdjList.end(); ch++){
           parents += (*ch)->label+" ";
        }
        cerr << "Node: " << (*node)->label << ", children: " << children;
        cerr << ", parents: " << parents << endl;
    }
}
//---------------------------------------------------------------------------
void printAttached(unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    unordered_map<unsignedPair, AbstractNode *>::iterator it;
    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
        cerr << it->first.first << "." << it->first.second << " : " << it->second << " ";
    }
    cerr << endl;
}
#endif
//---------------------------------------------------------------------------
