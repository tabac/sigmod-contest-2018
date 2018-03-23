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
using namespace std;
//---------------------------------------------------------------------------
int intermDataCounter = 0;
int targetCounter = 0;

//---------------------------------------------------------------------------
void Planner::updateAttached(unordered_map<unsignedPair, AbstractNode *> &lastAttached,
                             unsignedPair relationPair, AbstractNode *newNode)
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
void Planner::setSelections(SelectInfo &selection,
                            unordered_set<SelectInfo> &selections,
                            AbstractNode *node)
{
    assert(!node->inAdjList.empty());

    vector<AbstractNode *>::iterator it;
    for (it = node->inAdjList.begin(); it != node->inAdjList.end(); ++it) {
        assert(!(*it)->selections.empty());

        if ((*it)->isBaseRelation()) {
            // The parent node is a base relation.
            unsigned relId = (*it)->selections[0].relId;

            unordered_set<SelectInfo>::iterator st;
            for (st = selections.begin(); st != selections.end(); ++st) {
                if (((*st).relId == relId) &&
                    ((*st).relId == selection.relId) &&
                    ((*st).binding == selection.binding)) {

                    if (!Utils::contains(node->selections, (*st))) {
                        node->selections.emplace_back((*st));
                    }
                }
            }
        } else {
            // The parent node is not a base relation.
            vector<SelectInfo>::iterator jt;
            for (jt = (*it)->selections.begin(); jt != (*it)->selections.end(); ++jt) {
                if (!Utils::contains(node->selections, (*jt))) {
                    node->selections.emplace_back((*jt));
                }
            }
        }
    }

}
//---------------------------------------------------------------------------
void Planner::addFilter(Plan &plan, FilterInfo& filter,
                        unordered_set<SelectInfo> &selections,
                        unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    DataNode *dataNode = new DataNode();
    FilterOperatorNode *filterNode = new FilterOperatorNode(filter);

    unsignedPair filterPair = {filter.filterColumn.relId,
                               filter.filterColumn.binding};

    AbstractNode::connectNodes(lastAttached[filterPair], filterNode);
    AbstractNode::connectNodes(filterNode, dataNode);

    Planner::setSelections(filter.filterColumn, selections, filterNode);
    Planner::setSelections(filter.filterColumn, selections, dataNode);

    Planner::updateAttached(lastAttached, filterPair, dataNode);

    plan.nodes.push_back((AbstractNode *) filterNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

#ifndef NDEBUG
    filterNode->label = filterNode->info.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
#endif

}
//---------------------------------------------------------------------------
void Planner::addJoin(Plan& plan, PredicateInfo& predicate,
                      unordered_set<SelectInfo> &selections,
                      unordered_map<unsignedPair, AbstractNode *> &lastAttached)
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

    Planner::setSelections(predicate.left, selections, joinNode);
    Planner::setSelections(predicate.right, selections, joinNode);
    // Here `predicate.left` is ignored for sure...
    Planner::setSelections(predicate.left, selections, dataNode);

    Planner::updateAttached(lastAttached, leftPair, dataNode);
    Planner::updateAttached(lastAttached, rightPair, dataNode);

    plan.nodes.push_back((AbstractNode *) joinNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

#ifndef NDEBUG
    joinNode->label = joinNode->info.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
#endif

}
//---------------------------------------------------------------------------
void Planner::addFilterJoin(Plan& plan, PredicateInfo& predicate,
                            unordered_set<SelectInfo> &selections,
                            unordered_map<unsignedPair, AbstractNode *> &lastAttached)
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

    Planner::setSelections(predicate.left, selections, joinNode);
    Planner::setSelections(predicate.left, selections, dataNode);

    Planner::updateAttached(lastAttached, leftPair, dataNode);

    plan.nodes.push_back((AbstractNode *) joinNode);
    plan.nodes.push_back((AbstractNode *) dataNode);

#ifndef NDEBUG
    joinNode->label = joinNode->info.dumpLabel();
    dataNode->label = "d" + to_string(intermDataCounter++);
#endif

}
//---------------------------------------------------------------------------
void Planner::addAggregate(Plan &plan, QueryInfo& query,
                           unordered_map<unsignedPair, AbstractNode *> &lastAttached)
{
    DataNode *dataNode =  new DataNode();
    AggregateOperatorNode *aggregateNode = new AggregateOperatorNode();

    aggregateNode->selections = query.selections;
    dataNode->selections = query.selections;

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
void Planner::attachQueryPlan(Plan &plan, DataEngine &engine, QueryInfo &query)
{
    unordered_map<unsignedPair, AbstractNode *> lastAttached;

    // Push original relations.
    unsigned bd;
    vector<RelationId>::iterator rt;
    for (bd = 0, rt = query.relationIds.begin(); rt != query.relationIds.end(); ++rt, ++bd) {
        vector<AbstractNode *>::iterator lt;
        lt = find(plan.nodes.begin(), plan.nodes.end(), &engine.relations[(*rt)]);

        if (lt == plan.nodes.end()) {
            plan.nodes.push_back((AbstractNode *) &engine.relations[(*rt)]);

            AbstractNode::connectNodes(plan.root, plan.nodes.back());
            lastAttached[make_pair((*rt), bd)] = plan.nodes.back();
        } else {
            lastAttached[make_pair((*rt), bd)] = (*lt);
        }
    }

    // Get all selections used in the query.
    unordered_set<SelectInfo> selections;
    query.getAllSelections(selections);

    //TODO: sort filters by selectivity order.

    // Push filters.
    vector<FilterInfo>::iterator ft;
    for(ft = query.filters.begin(); ft != query.filters.end(); ++ft){
        // gmytil================================================================
//        uint64_t selec2 = engine.getFilterSelectivity(*ft);
//        cout << "Second Selectivity: " << selec2 << endl;
        //=======================================================================
        Planner::addFilter(plan, (*ft), selections, lastAttached);
    }

    //TODO: sort joins by selectivity order. Smaller goes first.

    // Push join predicates.
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
                Planner::addFilterJoin(plan, (*pt), selections, lastAttached);
            } else {
                unsignedPair leftPair = {(*pt).left.relId,
                                         (*pt).left.binding};
                unsignedPair rightPair = {(*pt).right.relId,
                                          (*pt).right.binding};

                if (lastAttached[leftPair] == lastAttached[rightPair]) {
                    Planner::addFilterJoin(plan, (*pt), selections, lastAttached);
                } else {
                    // If predicate refers to different tables
                    // add `JoinOperatorNode`.
                    Planner::addJoin(plan, (*pt), selections, lastAttached);
                }
            }
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

#ifndef NDEBUG
    root->label = "global_root";
    vector<AbstractNode *>::iterator jt;
    for (jt = plan->nodes.begin(); jt != plan->nodes.end(); ++jt) {
        assert((*jt)->status ==  NodeStatus::fresh);
        assert((*jt)->visited == 0);
    }
#endif

    return plan;
}
//---------------------------------------------------------------------------
#ifndef NDEBUG
void Planner::printPlan(Plan* plan)
{
    vector<AbstractNode*>::iterator node;
    for(node = plan->nodes.begin(); node != plan->nodes.end(); node++){
        cout << (*node)->label << ": ";

        vector<SelectInfo>::iterator jt;
        for (jt = (*node)->selections.begin(); jt != (*node)->selections.end(); ++jt) {
            cout << (*jt).dumpLabel() << " ";
        }
        cout << endl;
    }

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
#endif
//---------------------------------------------------------------------------
