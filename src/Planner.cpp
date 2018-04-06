#include <iostream>
#include <algorithm>
#include <queue>
#include <map>
#include "Planner.hpp"
#include "DataEngine.hpp"
#include "Plan.hpp"
#include "Parser.hpp"
#include "Relation.hpp"
#include "Utils.hpp"

#define FILTER_SELECT_THRES 0.5
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
void Planner::updateAttached(OriginTracker &lastAttached, const OTKey relationPair, AbstractNode *newNode)
{
    // Fail, total fail.
    AbstractNode *oldNode = lastAttached[relationPair];

    if (!oldNode->isBaseRelation()) {
        OriginTracker ::iterator jt;
        for (jt = lastAttached.begin(); jt != lastAttached.end(); ++jt) {
            if (jt->second == oldNode && get<2>(jt->first)==get<2>(relationPair)) {
                jt->second = newNode;
            }
        }
    } else {
        lastAttached[relationPair] = newNode;
    }
}
//---------------------------------------------------------------------------
static unsigned propagateSharedSelection(QueryInfo &query,AbstractOperatorNode *o,
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
                    if (o->queryId == query.queryId) {
                        q.push(o);
                    }
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

                if (count != 0 ) {
                    cur->selections.emplace_back(selection);

                    vector<AbstractNode *>::iterator jt;
                    for (jt = cur->outAdjList[0]->outAdjList.begin(); jt != cur->outAdjList[0]->outAdjList.end(); ++jt) {
                        AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);
                        if (o->queryId == query.queryId){
                            q.push(o);
                        }
                     }
                }
            }else {
                // offload queue
                while (!q.empty()) {
                    AbstractOperatorNode *cur = q.front();
                    q.pop();
                    if (!cur->outAdjList[0]->outAdjList.empty()) {

                        if (cur->hasSelection(selection)) {
                            --count;
                        }

                        if (count != 0) {
                            cur->selections.emplace_back(selection);
                        }
                    }
                }
            }
        }
    }
}
//---------------------------------------------------------------------------
static unsigned propagateSelection(QueryInfo &query, AbstractOperatorNode *o,
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

            if (count > 0) {
                o->selections.emplace_back(selection);

                o = (AbstractOperatorNode *) o->outAdjList[0]->outAdjList[0];
            } else {
                break;
            }
        }
    }

    return count;
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

    unordered_map<SelectInfo, unsigned>::iterator it;
    for (it = selectionsMap.begin(); it != selectionsMap.end(); ++it) {
        Relation *r = (Relation *) findRelationBySelection(plan, it->first);

        vector<AbstractNode *>::iterator jt;
        for (jt = r->outAdjList.begin(); jt != r->outAdjList.end(); ++jt) {
            AbstractOperatorNode *o = (AbstractOperatorNode *) (*jt);
            //cout << "TRYING TO ADD SELECTION "<< (it->first).dumpLabel() << " to Q"<<query.queryId << " in operator "<<(*o).label<<endl;

            //if ((o->queryId == query.queryId && o->hasBinding(it->first.binding))) { //|| Utils::contains(o->sharedQueries, query.queryId))

            if ((o->queryId == query.queryId || Utils::contains(o->sharedQueries, query.queryId)) && o->hasBinding(it->first.binding)){

                unsigned count;
                //count = propagateSelection(query, o, it->first, it->second);
               if(o->sharedQueries.empty()){
                    count = propagateSelection(query, o, it->first, it->second);
                }else{
                    count = propagateSharedSelection(query, o, it->first, it->second);
                }

                it->second -= count;
            }
        }
    }
}
//---------------------------------------------------------------------------

//unsigned Planner::addFilters(Plan &plan, QueryInfo& query, OriginTracker &lastAttached)
//{
//
//    unsigned counter = 0;
//    vector<FilterInfo>::iterator ft;
//    for(ft = query.filters.begin(); ft != query.filters.end(); ++ft){
//        float selectivity;
//        if ((selectivity = DataEngine::getFilterSelectivity(*ft)) <= FILTER_SELECT_THRES){
//#ifndef NDEBUG
//            cout << "FILTER SELECTIVITY " << selectivity << endl;
//#endif
//            DataNode *dataNode = new DataNode();
//            FilterOperatorNode *filterNode = new FilterOperatorNode((*ft));
//
//            unsignedPair filterPair = {ft->filterColumn.relId,
//                                       ft->filterColumn.binding};
//
//            AbstractNode::connectNodes(lastAttached[filterPair], filterNode);
//            AbstractNode::connectNodes(filterNode, dataNode);
//
//            Planner::updateAttached(lastAttached, filterPair, dataNode);
//
//            plan.nodes.push_back((AbstractNode *) filterNode);
//            plan.nodes.push_back((AbstractNode *) dataNode);
//            counter++;
//
//#ifndef NDEBUG
//            filterNode->label = filterNode->info.dumpLabel();
//            dataNode->label = "d" + to_string(intermDataCounter++);
//#endif
//        }else{
//            break;
//        }
//    }
//#ifndef NDEBUG
//    cout << "Start from counter " << counter << endl;
//#endif
//
//    return counter;
//}
//---------------------------------------------------------------------------
void Planner::addFilters2(Plan &plan, QueryInfo& query, OriginTracker &lastAttached)
{

    unsigned counter = 0;
    vector<FilterInfo>::iterator ft;
    for(ft = query.filters.begin(); ft != query.filters.end(); ++ft){
            DataNode *dataNode = new DataNode();
            FilterOperatorNode *filterNode = new FilterOperatorNode((*ft));
            filterNode->queryId = query.queryId;

            OTKey filterPair = {ft->filterColumn.relId, ft->filterColumn.binding, query.queryId};

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
                OTKey leftPair = {(*pt).left.relId, (*pt).left.binding, query.queryId};
                OTKey rightPair = {(*pt).right.relId, (*pt).right.binding, query.queryId};

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
                OTKey leftPair = {(*pt).left.relId, (*pt).left.binding,query.queryId};
                OTKey rightPair = {(*pt).right.relId, (*pt).right.binding,query.queryId};

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
    joinNode->queryId = query.queryId;
    //(joinNode->sharedQueries).push_back(query.queryId);

    OTKey leftPair = {predicate.left.relId, predicate.left.binding,query.queryId};
    OTKey rightPair = {predicate.right.relId, predicate.right.binding,query.queryId};

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

    SharedJoinOperatorNode* joinNode;
    DataNode *dataNode;

    OTKey leftPair = {predicate.left.relId, predicate.left.binding,query.queryId};
    OTKey rightPair = {predicate.right.relId, predicate.right.binding,query.queryId};

    try {
        joinNode = plan.sharedJoins.at(predicate);
        auto nhandler = plan.sharedJoins.extract(predicate);
        joinNode->updateBindings(predicate);
        (joinNode->sharedQueries).push_back(query.queryId);
        plan.sharedJoins.insert(move(nhandler));
        dataNode = (DataNode*) joinNode->outAdjList[0];
    }
    catch (const out_of_range &) {
        joinNode = new SharedJoinOperatorNode(predicate);
        (joinNode->sharedQueries).push_back(query.queryId);
        dataNode = new DataNode();

        #ifndef NDEBUG
            joinNode->label = predicate.dumpLabel();
            dataNode->label = "d" + to_string(intermDataCounter++);
        #endif

        AbstractNode::connectNodes(lastAttached[leftPair], joinNode);
        AbstractNode::connectNodes(lastAttached[rightPair], joinNode);
        AbstractNode::connectNodes(joinNode, dataNode);

        plan.nodes.push_back((AbstractNode *) joinNode);
        plan.nodes.push_back((AbstractNode *) dataNode);

        plan.sharedJoins[predicate] = joinNode;
    }

    Planner::updateAttached(lastAttached, leftPair, dataNode);
    Planner::updateAttached(lastAttached, rightPair, dataNode);

}
//---------------------------------------------------------------------------
void Planner::addFilterJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query, OriginTracker &lastAttached)
{
    DataNode *dataNode = new DataNode();
    FilterJoinOperatorNode *joinNode = new FilterJoinOperatorNode(predicate);
    joinNode->queryId = query.queryId;
    //(joinNode->sharedQueries).push_back(query.queryId);

    OTKey leftPair = {predicate.left.relId, predicate.left.binding,query.queryId};
    OTKey rightPair = {predicate.right.relId, predicate.right.binding,query.queryId};

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
    //(aggregateNode->sharedQueries).push_back(query.queryId);
    aggregateNode->selections = query.selections;

    AbstractNode::connectNodes((AbstractNode *) aggregateNode,
                               (AbstractNode *) dataNode);

    OriginTracker ::iterator it;
    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
        if ((*it).second->outAdjList.empty() && get<2>((*it).first)==query.queryId) {
            AbstractNode::connectNodes((*it).second, (AbstractNode *) aggregateNode);
        }
    }

    plan.nodes.push_back((AbstractNode *) aggregateNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

    plan.exitNodes.push_back(dataNode);

#ifndef NDEBUG
    // the assertion below holds only if we have a `lastAttach` per query

//    AbstractNode *anode = (*lastAttached.begin()).second;
//    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
//        assert(anode == (*it).second);
//    }
    aggregateNode->label = "aggr";
    dataNode->label = "t" + to_string(targetCounter++);
#endif

}
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
            lastAttached[make_tuple((*rt), bd, query.queryId)] = plan.nodes.back();
        } else {
            lastAttached[make_tuple((*rt), bd, query.queryId)] = (*lt);
        }
    }
    return lastAttached;
}
//---------------------------------------------------------------------------
void Planner::attachQueryPlanShared(Plan &plan, QueryInfo &query, OriginTracker& lastAttached)
{
    // push filters
    Planner::addFilters2(plan, query, lastAttached);

    // Push join predicates.
    Planner::addNonSharedJoins(plan, query, lastAttached);
    //Planner::addJoins(plan, query, lastAttached);
    Planner::addAggregate(plan, query, lastAttached);
}
//---------------------------------------------------------------------------
vector<PredicateInfo> Planner::findCommonJoins(vector<QueryInfo> &batch)
{
    CommonJoinCounter commonJoins;
    vector<QueryInfo>::iterator it;
    for(it = batch.begin(); it != batch.end(); ++it) {
        for(vector<PredicateInfo>::iterator pt = (*it).predicates.begin(); pt != (*it).predicates.end(); pt++){
            // if it is a pure join and not a filter join
            if (!((*pt).left.relId == (*pt).right.relId && (*pt).left.binding == (*pt).right.binding)){
                // check if there is a filter on the left or right relation
                bool skip = false;
                for(vector<FilterInfo>::iterator ft = (*it).filters.begin();ft != (*it).filters.end(); ft++){
                    if(ft->filterColumn == (*pt).left || ft->filterColumn == (*pt).right){
                        // join is filtered and should not be considered as joined
                        skip = true;
                    }
                }

                if(skip) continue;

                try {
                    commonJoins.at(*pt) += 1;
                }
                catch (const out_of_range &) {
                    commonJoins[*pt] = 1;
                }
            }
        }
    }

    vector<RelationId > alreadyShared;
    vector<PredicateInfo> sharedJoins;

    for(CommonJoinCounter::iterator ci = commonJoins.begin(); ci != commonJoins.end(); ci++){
        if(ci->second > 1 && !Utils::contains(alreadyShared, (ci->first).left.relId) &&
                !Utils::contains(alreadyShared, (ci->first).right.relId)){

            alreadyShared.push_back((ci->first).left.relId);
            alreadyShared.push_back((ci->first).right.relId);
            sharedJoins.push_back(ci->first);
        }
    }

    return sharedJoins;
};
//---------------------------------------------------------------------------
Plan* Planner::generatePlan(vector<QueryInfo> &queries)
{
    Plan *plan = new Plan();
    AbstractNode* root = new DataNode();

    plan->nodes.push_back(root);
    plan->root = root;

    plan->commonJoins = Planner::findCommonJoins(queries);

    // first add shared joins
    OriginTracker lastAttached;
    vector<QueryInfo>::iterator it;
    for(it = queries.begin(); it != queries.end(); ++it) {
        connectQueryBaseRelations(*plan, *it, lastAttached);

        if(!(plan->commonJoins).empty())
        {
            // check if this query contains one of the shared joins and push it first
            for (vector<PredicateInfo>::iterator pt = (*it).predicates.begin(); pt != (*it).predicates.end(); pt++) {
                if (*pt == (plan->commonJoins).back()) {
                    //cout << "ADD the Shared " << (*pt).dumpLabel() << endl;
                    Planner::addSharedJoin(*plan, *pt, (*it), lastAttached);
                }
            }
        }
//        Planner::attachQueryPlanShared(*plan, (*it), lastAttached);
//        setQuerySelections(*plan, *it);
    }


    for(it = queries.begin(); it != queries.end(); ++it) {
        Planner::attachQueryPlanShared(*plan, (*it), lastAttached);
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

    for(it = queries.begin(); it != queries.end(); ++it) {
        setQuerySelections(*plan, *it);
    }

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
