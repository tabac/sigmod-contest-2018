#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include "Plan.hpp"
#include "Parser.hpp"
#include "Planner.hpp"
#include "Relation.hpp"
#include "Utils.hpp"
//---------------------------------------------------------------------------
using namespace std; //---------------------------------------------------------------------------
int intermDataCounter = 0;
int targetCounter = 0;
//---------------------------------------------------------------------------
void Planner::addBaseRelationNode(RelationId& r, DataEngine& engine, Plan *splan, unordered_map<RelationId, vector<SelectInfo>>& colsToPush,
unordered_map<RelationId, AbstractNode*>& lastAttached)
{
    AbstractNode* d = new Relation(engine.relations[r]);
    d->label = "r"+std::to_string(r);
    lastAttached[r] = d;
    colsToPush[r];
    splan->nodes.push_back(d);
    // splan->baseRelations.push_back(d);
}
//---------------------------------------------------------------------------
void Planner::addFilterNode(FilterInfo& f, Plan *splan, unordered_map<RelationId, vector<SelectInfo>>& colsToPush,
unordered_map<RelationId, AbstractNode*>& lastAttached)
{
    FilterOperatorNode *filterOp = new FilterOperatorNode(f);
    filterOp->label=filterOp->info.dumpLabel();
    RelationId& frel = filterOp->info.filterColumn.relId;
    //bool cond1 = find(splan->baseRelations.begin(),splan->baseRelations.end(), lastAttached[frel]) != splan->baseRelations.end();
    //bool cond2 = find(lastAttached[frel]->outAdjList.begin(),lastAttached[frel]->outAdjList.end(), filterOp) != lastAttached[frel]->outAdjList.end();
    //if(cond1 && cond2){
    //    delete filterOp;
    //    break;
    //}

    // For the case where filters are always applied first
    // the next datanode needs to contain teh following columns:
    // a) the column where the filter is applied
    // b) any join column on that relation
    // c) the final aggregates of that relation
    vector<SelectInfo>::iterator pushedCol;
    for(pushedCol = colsToPush.at(frel).begin(); pushedCol != colsToPush.at(frel).end(); pushedCol++){
        filterOp->selectionsInfo.push_back(*pushedCol);
    }
    AbstractNode *intermData =  new DataNode();
    intermData->label="d"+std::to_string(intermDataCounter);
    intermDataCounter++;
    AbstractNode *filterNode = (AbstractNode *) filterOp;
    intermData -> inAdjList.push_back(filterNode);
    filterOp -> outAdjList.push_back(intermData);
    filterOp -> inAdjList.push_back(lastAttached.at(frel));
    lastAttached.at(frel) -> outAdjList.push_back(filterNode);
    lastAttached[frel] = intermData;
    splan->nodes.push_back(filterNode);
    splan->nodes.push_back(intermData); // create a datanode for the filter output
}
//---------------------------------------------------------------------------
void Planner::addJoinNode(PredicateInfo& p, Plan *splan, unordered_map<RelationId, vector<SelectInfo>>& colsToPush,
unordered_map<RelationId, AbstractNode*>& lastAttached)
{
    JoinOperatorNode *joinOp = new JoinOperatorNode(p);
    joinOp->label=joinOp->info.dumpLabel();
    RelationId& jRelLeft = joinOp->info.left.relId;
    RelationId& jRelRight = joinOp->info.right.relId;
    //bool cond1 = find(splan->baseRelations.begin(),splan->baseRelations.end(), lastAttached[jRelLeft]) != splan->baseRelations.end();
    //bool cond2 = nodeMatching(lastAttached[jRelLeft]->outAdjList, joinOp->label);
    //bool cond3 = find(splan->baseRelations.begin(),splan->baseRelations.end(), lastAttached[jRelRight]) != splan->baseRelations.end();
    //bool cond4 = nodeMatching(lastAttached[jRelRight]->outAdjList, joinOp->label);
    //if(cond1 && cond2 && cond3 && cond4) {
    //    delete joinOp;
    //    break;
    //}

    // For the case where filters are always applied first
    // the next datanode needs to contain teh following columns:
    // a) any join column on that relation
    // b) the final aggregates of that relation
    //TODO: for now I also pass the filter columns that need to be pruned
    vector<SelectInfo>::iterator pushedCol;
    for(pushedCol = colsToPush.at(jRelLeft).begin(); pushedCol != colsToPush.at(jRelLeft).end(); pushedCol++){
        joinOp->selectionsInfo.push_back(*pushedCol);
    }
    for(pushedCol = colsToPush.at(jRelRight).begin(); pushedCol != colsToPush.at(jRelRight).end(); pushedCol++){
        joinOp->selectionsInfo.push_back(*pushedCol);
    }
    AbstractNode *intermData =  new DataNode();
    intermData->label="d"+std::to_string(intermDataCounter);
    intermDataCounter++;
    intermData -> inAdjList.push_back((AbstractNode *) joinOp);
    joinOp -> outAdjList.push_back(intermData);
    joinOp -> inAdjList.push_back(lastAttached.at(jRelLeft));
    joinOp -> inAdjList.push_back(lastAttached.at(jRelRight));
    lastAttached.at(jRelLeft) -> outAdjList.push_back(joinOp);
    lastAttached.at(jRelRight) -> outAdjList.push_back(joinOp);
    lastAttached[jRelLeft] = intermData;
    lastAttached[jRelRight] = intermData;
    splan->nodes.push_back((AbstractNode *) joinOp);
    splan->nodes.push_back(intermData);
}
//---------------------------------------------------------------------------
void Planner::addAggregateNode(Plan &plan, QueryInfo& query,
                               unordered_map<RelationId, AbstractNode*>& lastAttached)
{
    DataNode *dataNode =  new DataNode();
    AggregateOperatorNode *aggregateNode = new AggregateOperatorNode();

    aggregateNode->selectionsInfo = query.selections;

    AbstractNode::connectNodes((AbstractNode *) aggregateNode,
                               (AbstractNode *) dataNode);
    AbstractNode::connectNodes((AbstractNode *) lastAttached.at(query.selections[0].relId),
                               (AbstractNode *) aggregateNode);

    plan.nodes.push_back((AbstractNode *) aggregateNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

    plan.exitNodes.push_back(dataNode);

    /////////////////////////////////////////////////////////////////////////
    /// FOR DEBUG PURPOSES
    aggregateNode->label="aggr";
    dataNode->label="t"+std::to_string(targetCounter++);
    /////////////////////////////////////////////////////////////////////////
}
//---------------------------------------------------------------------------
Plan* Planner::generateSingleQueryPlan(DataEngine &engine, QueryInfo &q)
{
    // dummy plan. Apply first all the filters
    Plan *splan = new Plan();
    unordered_map<RelationId, vector<SelectInfo>> colsToPush;
    unordered_map<RelationId, AbstractNode*> lastAttached;
    vector<FilterInfo>::iterator f;
    vector<PredicateInfo>::iterator p;
    vector<SelectInfo>::iterator s;
    vector<RelationId>::iterator r;

    for(r = q.relationIds.begin(); r != q.relationIds.end(); r++){
        addBaseRelationNode(*r, engine, splan, colsToPush, lastAttached);
    }

    // find columns that need to be pushed for each relation
    for(f = q.filters.begin(); f != q.filters.end(); f++){
        if(!Utils::contains(colsToPush.at(f->filterColumn.relId), f->filterColumn)){
            colsToPush[f->filterColumn.relId].push_back(f->filterColumn);
        }
    }
    for(p = q.predicates.begin(); p != q.predicates.end(); p++){
        SelectInfo& leftRelation = p->left;
        SelectInfo& rightRelation = p->right;
        if(!Utils::contains(colsToPush.at(leftRelation.relId), leftRelation)) {
            colsToPush[leftRelation.relId].push_back(leftRelation);
        }
        if(!Utils::contains(colsToPush.at(rightRelation.relId), rightRelation)) {
            colsToPush[rightRelation.relId].push_back(rightRelation);
        }
    }
    for(s = q.selections.begin(); s != q.selections.end(); s++){
        if(!Utils::contains(colsToPush.at(s->relId), *s)) {
            colsToPush[s->relId].push_back(*s);
        }
    }

    // Apply Filters
    for(f = q.filters.begin(); f != q.filters.end(); f++){
        addFilterNode(*f, splan, colsToPush, lastAttached);
    }
   // Apply Join Predicates
    for(p = q.predicates.begin(); p != q.predicates.end(); p++){
        addJoinNode(*p,splan,colsToPush,lastAttached);
    }
    // Apply final aggregate selections
    addAggregateNode(*splan, q, lastAttached);

    return splan;
}
//---------------------------------------------------------------------------
/*
Plan* Planner::generatePlan(DataEngine &engine, vector<QueryInfo> &queries)
{
    Plan *p = new Plan();
    AbstractNode* globalRoot = new DataNode();
    globalRoot->label="global_root";
    p->nodes.push_back(globalRoot);
    p->root = globalRoot;

    vector<QueryInfo>::iterator currentQuery;
    for(currentQuery = queries.begin(); currentQuery != queries.end(); currentQuery++){
        // connect the base relations of individual query to global dummy root
        Plan *singlePlan = generateSingleQueryPlan(engine, *currentQuery);
        cout << endl;
        cout << endl;
        vector<AbstractNode *>::iterator queryBaseRel;
        for(queryBaseRel = singlePlan->baseRelations.begin(); queryBaseRel != singlePlan->baseRelations.end(); queryBaseRel++){
        //    cout << "Base Rel: " << (*queryBaseRel)->label << endl;
        //    if(find(p->nodes[0]->outAdjList.begin(), p->nodes[0]->outAdjList.end(), *queryBaseRel) == p->nodes[0]->outAdjList.end()){
                p->nodes[0]->outAdjList.push_back(*queryBaseRel);
                (*queryBaseRel)->inAdjList.push_back(p->nodes[0]);
        //    }
        }
        // add the nodes of individual query to the global plan graph
        vector<AbstractNode *>::iterator splanNode;
        for(splanNode = singlePlan->nodes.begin(); splanNode != singlePlan->nodes.end(); splanNode++){
            p->nodes.push_back(*splanNode);
        }

        p->exitNodes.push_back(singlePlan->exitNodes[0]);
    }
    printPlanGraph(p);
    return p;
}
*/
//---------------------------------------------------------------------------
void Planner::printPlanGraph(Plan* plan)
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
        cout << "Node: " << (*node)->label << ", children: " << children;
        cout << ", parents: " << parents << endl;
    }
}
//---------------------------------------------------------------------------
void printAttached(unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    unordered_map<unsignedPair, AbstractNode *>::iterator it;
    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
        cout << it->first.first << "." << it->first.second << " : " << it->second << " ";
    }
    cout << endl;
}
//---------------------------------------------------------------------------
void Planner::updateAttached(unordered_map<unsignedPair, AbstractNode *> &lastAttached,
                             AbstractNode *oldNode, AbstractNode *newNode)
{
    unordered_map<unsignedPair, AbstractNode *>::iterator it;
    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
        if (it->second == oldNode) {
            it->second = newNode;
        }
    }
 }
//---------------------------------------------------------------------------
void Planner::addFilter(Plan &plan, FilterInfo& filter,
                        unordered_set<SelectInfo> selections,
                        unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    DataNode *dataNode = new DataNode();
    FilterOperatorNode *filterNode = new FilterOperatorNode(filter);

    unsignedPair filterPair = {filter.filterColumn.relId,
                               filter.filterColumn.binding};

    // TODO: This copies all the selections to every node. Maybe
    //       we can do something better here.
    unordered_set<SelectInfo>::iterator st;
    for (st = selections.begin(); st != selections.end(); ++st) {
        filterNode->selectionsInfo.emplace_back((*st));
    }

    AbstractNode::connectNodes(lastAttached[filterPair], filterNode);
    AbstractNode::connectNodes(filterNode, dataNode);

    Planner::updateAttached(lastAttached, lastAttached[filterPair], dataNode);

    plan.nodes.push_back((AbstractNode *) filterNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

    /////////////////////////////////////////////////////////////////////////
    /// FOR DEBUG PURPOSES
    filterNode->label = filterNode->info.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
    /////////////////////////////////////////////////////////////////////////
}
//---------------------------------------------------------------------------
void Planner::addJoin(Plan& plan, PredicateInfo& predicate,
                      unordered_set<SelectInfo> selections,
                      unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    DataNode *dataNode = new DataNode();
    JoinOperatorNode *joinNode = new JoinOperatorNode(predicate);

    unsignedPair leftPair = {predicate.left.relId,
                             predicate.left.binding};
    unsignedPair rightPair = {predicate.right.relId,
                              predicate.right.binding};

    // TODO: This copies all the selections to every node. Maybe
    //       we can do something better here.
    unordered_set<SelectInfo>::iterator st;
    for (st = selections.begin(); st != selections.end(); ++st) {
        joinNode->selectionsInfo.emplace_back((*st));
    }

    AbstractNode::connectNodes(lastAttached[leftPair], joinNode);
    AbstractNode::connectNodes(lastAttached[rightPair], joinNode);
    AbstractNode::connectNodes(joinNode, dataNode);

    Planner::updateAttached(lastAttached, lastAttached[leftPair], dataNode);
    Planner::updateAttached(lastAttached, lastAttached[rightPair], dataNode);

    plan.nodes.push_back((AbstractNode *) joinNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

    /////////////////////////////////////////////////////////////////////////
    /// FOR DEBUG PURPOSES
    joinNode->label = joinNode->info.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
    /////////////////////////////////////////////////////////////////////////
}
//---------------------------------------------------------------------------
void Planner::addFilterJoin(Plan& plan, PredicateInfo& predicate,
                            unordered_set<SelectInfo> selections,
                            unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    DataNode *dataNode = new DataNode();
    FilterJoinOperatorNode *joinNode = new FilterJoinOperatorNode(predicate);

    unsignedPair leftPair = {predicate.left.relId,
                             predicate.left.binding};
    unsignedPair rightPair = {predicate.right.relId,
                              predicate.right.binding};

    assert(leftPair == rightPair);

    // TODO: This copies all the selections to every node. Maybe
    //       we can do something better here.
    unordered_set<SelectInfo>::iterator st;
    for (st = selections.begin(); st != selections.end(); ++st) {
        joinNode->selectionsInfo.emplace_back((*st));
    }

    AbstractNode::connectNodes(lastAttached[leftPair], joinNode);
    AbstractNode::connectNodes(joinNode, dataNode);

    Planner::updateAttached(lastAttached, lastAttached[leftPair], dataNode);

    plan.nodes.push_back((AbstractNode *) joinNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

    /////////////////////////////////////////////////////////////////////////
    /// FOR DEBUG PURPOSES
    joinNode->label = joinNode->info.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
    /////////////////////////////////////////////////////////////////////////
}//---------------------------------------------------------------------------
void Planner::addAggregate(Plan &plan, QueryInfo& query,
                           unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    DataNode *dataNode =  new DataNode();
    AggregateOperatorNode *aggregateNode = new AggregateOperatorNode();

    aggregateNode->selectionsInfo = query.selections;

    AbstractNode *anode = (*lastAttached.begin()).second;

    AbstractNode::connectNodes((AbstractNode *) aggregateNode,
                               (AbstractNode *) dataNode);

    AbstractNode::connectNodes((AbstractNode *) anode,
                               (AbstractNode *) aggregateNode);

    plan.nodes.push_back((AbstractNode *) aggregateNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

    plan.exitNodes.push_back(dataNode);

    /////////////////////////////////////////////////////////////////////////
    /// FOR DEBUG PURPOSES
    unordered_map<unsignedPair, AbstractNode *>::iterator it;
    for (it = lastAttached.begin(); it != lastAttached.end(); ++it) {
        assert(anode == (*it).second);
    }
    aggregateNode->label = "aggr";
    dataNode->label = "t" + to_string(targetCounter++);
    /////////////////////////////////////////////////////////////////////////
}
//---------------------------------------------------------------------------
void Planner::attachQueryPlan(Plan &plan, DataEngine &engine, QueryInfo &query)
{
    unordered_map<unsignedPair, AbstractNode *> lastAttached;

    // Push original relations.
    // TODO: We could check for already added relations
    //       and use the same pointer.
    unsigned bd;
    vector<RelationId>::iterator rt;
    for (bd = 0, rt = query.relationIds.begin(); rt != query.relationIds.end(); ++rt, ++bd) {
        vector<AbstractNode *>::iterator lt;
        lt = find(plan.relations.begin(), plan.relations.end(), &engine.relations[(*rt)]);

        if (lt == plan.relations.end()) {
            plan.nodes.push_back((AbstractNode *) &engine.relations[(*rt)]);
            plan.relations.push_back((AbstractNode *) &engine.relations[(*rt)]);

            AbstractNode::connectNodes(plan.root, plan.nodes.back());
            lastAttached[make_pair((*rt), bd)] = plan.nodes.back();
        } else {
            lastAttached[make_pair((*rt), bd)] = (*lt);
        }

        /////////////////////////////////////////////////////////////////////////
        /// FOR DEBUG PURPOSES
        engine.relations[(*rt)].label = "r" + to_string((*rt));
        /////////////////////////////////////////////////////////////////////////
    }

    // Get all selections used in the query.
    unordered_set<SelectInfo> selections;
    query.getAllSelections(selections);

    // Push filters.
    vector<FilterInfo>::iterator ft;
    for(ft = query.filters.begin(); ft != query.filters.end(); ++ft){
        Planner::addFilter(plan, (*ft), selections, lastAttached);
    }

    // Push join predicates.
    vector<PredicateInfo>::iterator pt;
    for(pt = query.predicates.begin(); pt != query.predicates.end(); ++pt) {
        if ((*pt).left.relId == (*pt).right.relId &&
            (*pt).left.binding == (*pt).right.binding) {
            Planner::addFilterJoin(plan, (*pt), selections, lastAttached);
        } else {
            Planner::addJoin(plan, (*pt), selections, lastAttached);
        }
    }

    Planner::addAggregate(plan, query, lastAttached);
}
//---------------------------------------------------------------------------
Plan* Planner::generatePlan(DataEngine &engine, vector<QueryInfo> &queries)
{
    Plan *plan = new Plan();
    AbstractNode* root = new DataNode();

    plan->nodes.push_back(root);
    plan->root = root;

    vector<QueryInfo>::iterator it;
    for(it = queries.begin(); it != queries.end(); ++it) {
        Planner::attachQueryPlan(*plan, engine, (*it));
    }

    /////////////////////////////////////////////////////////////////////////
    /// FOR DEBUG PURPOSES
    root->label = "global_root";
    vector<AbstractNode *>::iterator jt;
    for (jt = plan->nodes.begin(); jt != plan->nodes.end(); ++jt) {
        assert((*jt)->status ==  NodeStatus::fresh);
        assert((*jt)->visited == 0);
    }
    /////////////////////////////////////////////////////////////////////////

    return plan;
}
//---------------------------------------------------------------------------
