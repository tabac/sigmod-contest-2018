#include <iostream>
#include <algorithm>
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
static void recursivePropagateSelection(QueryInfo &query, AbstractOperatorNode *o,
                               const SelectInfo &selection, unsigned count)
{
    assert(o->hasBinding(selection.binding));
    if (Utils::contains(query.selections, selection)) {
        if (!o->outAdjList[0]->outAdjList.empty()) {
            o->selections.emplace_back(selection);

            // mias kai twra exw shared pragmata, mporei ena datanode na exei > 1 outedges. opote prepei
            // na psaksw gia to swsto.

            vector<AbstractNode *>::iterator jt;
            for (jt = o->outAdjList[0]->outAdjList.begin(); jt != o->outAdjList[0]->outAdjList.end(); ++jt) {
                AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);

                if (o->hasBinding(selection.binding)) {
                    recursivePropagateSelection(query, o, selection, count);
                }
            }
        }
    } else {
        if (!o->outAdjList[0]->outAdjList.empty()) {
            if (o->hasSelection(selection)) {
                --count;
            }

            if (count != 0) {
                o->selections.emplace_back(selection);

                vector<AbstractNode *>::iterator jt;
                for (jt = o->outAdjList[0]->outAdjList.begin(); jt != o->outAdjList[0]->outAdjList.end(); ++jt) {
                    AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);

                    if (o->hasBinding(selection.binding)) {
                        recursivePropagateSelection(query, o, selection, count);
                    }
                }
            } else {
                return;
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
                recursivePropagateSelection(query, o, it->first, it->second);
            }
        }
    }
}
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

        if(plan.sharedJoins.find(*pt)!=plan.sharedJoins.end()){
            continue;
        }
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

    if(plan.sharedJoins.find(predicate)!=plan.sharedJoins.end()){
        joinNode = plan.sharedJoins.at(predicate);
        joinNode->updateBindings(predicate);
        dataNode = (DataNode*) joinNode->outAdjList[0];
    }else{
        //PredicateInfo cjoin = PredicateInfo(ctr->first);
        //cout << cjoin.dumpLabel() << " APPEARED " << ctr->second << " times" << endl;
        JoinOperatorNode* joinNode = new JoinOperatorNode(predicate);
        dataNode = new DataNode();
        plan.sharedJoins[predicate] = joinNode;

        AbstractNode::connectNodes(joinNode, dataNode);
        plan.nodes.push_back((AbstractNode *) joinNode);
        plan.nodes.push_back((AbstractNode *) dataNode);
    }

    unsignedPair leftPair = {predicate.left.relId,
                             predicate.left.binding};
    unsignedPair rightPair = {predicate.right.relId,
                              predicate.right.binding};

    AbstractNode::connectNodes(lastAttached[leftPair], joinNode);
    AbstractNode::connectNodes(lastAttached[rightPair], joinNode);

    Planner::updateAttached(lastAttached, leftPair, dataNode);
    Planner::updateAttached(lastAttached, rightPair, dataNode);

#ifndef NDEBUG
    joinNode->label = predicate.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
#endif

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
void Planner::attachQueryPlanShared(Plan &plan, QueryInfo &query)
{

    OriginTracker lastAttached = Planner::connectQueryBaseRelations(plan, query);

    // check if this query contains one of the shared joins and push it first
    for(vector<PredicateInfo>::iterator pt = query.predicates.begin(); pt != query.predicates.end(); pt++){
        if(plan.commonJoins.find(*pt)!=plan.commonJoins.end() && plan.commonJoins.at(*pt) > 1){
            cout << "SHARED JOIN FOUND" << endl;
           Planner::addSharedJoin(plan, *pt, query, lastAttached);
        }
    }

    // push filters
    Planner::addFilters2(plan, query, lastAttached);

    // push remaining joins in order
    sort(query.predicates.begin(), query.predicates.end(), predicateComparator);

    // Push join predicates.
    Planner::addNonSharedJoins(plan, query, lastAttached);

    // Push aggregate.
    Planner::addAggregate(plan, query, lastAttached);

    // Setup selections for OperatorNodes.
    setQuerySelections(plan, query);
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
                // if this join does not exist in the catalog
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

//    for(CommonJoinCounter::iterator ctr = commonJoins.begin(); ctr != commonJoins.end(); ctr++){
//        if(ctr->second > 1){
//            PredicateInfo cjoin = PredicateInfo(ctr->first);
//            //cout << cjoin.dumpLabel() << " APPEARED " << ctr->second << " times" << endl;
//            sharedJoins[ctr->first] = new JoinOperatorNode(cjoin);
//        }
//    }

    vector<QueryInfo>::iterator it;
    for(it = queries.begin(); it != queries.end(); ++it) {
        Planner::attachQueryPlanShared(*plan, (*it));
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
