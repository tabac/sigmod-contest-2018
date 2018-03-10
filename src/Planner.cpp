#include <iostream>
#include <vector>
#include <map>
#include "Parser.hpp"
#include "Planner.hpp"
#include "Relation.hpp"
#include "Utils.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
Plan* Planner::generateSingleQueryPlan(DataEngine &engine, QueryInfo &q){
    // dummy plan. Apply first all the filters
    Plan *splan = new Plan();
    map<RelationId, vector<SelectInfo>> colsToPush;
    map<RelationId, AbstractNode*> lastAttached;

    vector<RelationId>::iterator r;
    for(r = q.relationIds.begin(); r != q.relationIds.end(); r++){
        AbstractNode* d = &engine.relations[*r];
        d -> setNodeId(0);
        lastAttached[*r] = d;
        splan->nodes.push_back((AbstractNode *) d);
        splan->baseRelations.push_back((AbstractNode *) d);
    }
 
    vector<FilterInfo>::iterator f;
    vector<PredicateInfo>::iterator p;
    vector<SelectInfo>::iterator s;

    // find columns that need to be pushed for each relation
    for(f = q.filters.begin(); f != q.filters.end(); f++){
        if(!Utils::contains(colsToPush.at(f->filterColumn.relId), f->filterColumn)){ 
            colsToPush.at(f->filterColumn.relId).push_back(f->filterColumn);
        }
    }

    for(p = q.predicates.begin(); p != q.predicates.end(); p++){
        SelectInfo& leftRelation = p->left;
        SelectInfo& rightRelation = p->right;
        if(!Utils::contains(colsToPush.at(leftRelation.relId), leftRelation)) {
            colsToPush.at(leftRelation.relId).push_back(leftRelation);
        }
        if(!Utils::contains(colsToPush.at(rightRelation.relId), rightRelation)) {
            colsToPush.at(rightRelation.relId).push_back(rightRelation);
        }
    }
    for(s = q.selections.begin(); s != q.selections.end(); s++){
        if(!Utils::contains(colsToPush.at(s->relId), *s)) {
            colsToPush.at(s->relId).push_back(*s);
        }
    }

    // Apply Filters

    for(f = q.filters.begin(); f != q.filters.end(); f++){
    	FilterOperatorNode *filterOp = new FilterOperatorNode(*f);
        filterOp->setNodeId(2);
        RelationId& frel = filterOp->info.filterColumn.relId;
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
        intermData->setNodeId(1);
        intermData -> inAdjList.push_back((AbstractNode *) filterOp);
        filterOp -> outAdjList.push_back(intermData);
        filterOp -> inAdjList.push_back(lastAttached.at(frel));
        lastAttached.at(frel) -> outAdjList.push_back(filterOp);
        lastAttached[frel] = intermData;
        splan->nodes.push_back((AbstractNode *) filterOp);
        splan->nodes.push_back(intermData); // create a datanode for the filter output 
    }

   // Apply Predicates
  
    for(p = q.predicates.begin(); p != q.predicates.end(); p++){
    	JoinOperatorNode *joinOp = new JoinOperatorNode(*p);
        joinOp->setNodeId(3);
        RelationId& jRelLeft = joinOp->info.left.relId;
        RelationId& jRelRight = joinOp->info.right.relId;
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
        intermData->setNodeId(1);
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

    // Apply final aggregate selections

    for(s = q.selections.begin(); s != q.selections.end(); s++){
    	AggregateOperatorNode *aggrOp = new AggregateOperatorNode();
	aggrOp->setNodeId(4);
        // For the case where filters are always applied first
        // the next datanode needs to contain teh following columns:
        // a) the column where the aggregate selection exists
        aggrOp->selectionsInfo.push_back(*s);
        AbstractNode *finalData =  new DataNode();
        finalData->setNodeId(5);
        finalData -> inAdjList.push_back((AbstractNode *) aggrOp);
        aggrOp -> outAdjList.push_back(finalData);
        aggrOp -> inAdjList.push_back(lastAttached.at(s->relId));
        lastAttached.at(s->relId) -> outAdjList.push_back(aggrOp);
        splan->nodes.push_back((AbstractNode *) aggrOp);
        splan->nodes.push_back(finalData);
    }

    return splan;
}
//---------------------------------------------------------------------------
Plan* Planner::generatePlan(DataEngine &engine, vector<QueryInfo> &queries)
{
    Plan *p = new Plan();
    AbstractNode* globalRoot = new DataNode();
    globalRoot->setNodeId(-1);
    p->nodes.push_back(globalRoot);

    vector<QueryInfo>::iterator currentQuery;
    for(currentQuery = queries.begin(); currentQuery != queries.end(); currentQuery++){
        // connect the base relations of individual query to global dummy root
        Plan *singlePlan = generateSingleQueryPlan(engine, *currentQuery);
        vector<AbstractNode *>::iterator queryBaseRel;
        for(queryBaseRel = singlePlan->baseRelations.begin(); queryBaseRel != singlePlan->baseRelations.end(); queryBaseRel++){
            p->nodes[0]->outAdjList.push_back(*queryBaseRel);
            (*queryBaseRel)->inAdjList.push_back(p->nodes[0]);
        }
        // add the nodes of individual query to the global plan graph
        vector<AbstractNode *>::iterator splanNode;
        for(splanNode = singlePlan->nodes.begin(); splanNode != singlePlan->nodes.end(); splanNode++){
            p->nodes.push_back(*splanNode);
        }
    }
    printPlanGraph(p->nodes[0]);
    return p;
}
//---------------------------------------------------------------------------
void Planner::printPlanGraph(AbstractNode* currentNode)
{
  cout << "Print the plan:" << endl; 
  cout << "Node ID: " << currentNode->nodeId << endl;
  cout << "Children: " << endl;
  vector<AbstractNode *>::iterator childNode;
  for(childNode = currentNode->outAdjList.begin(); childNode != currentNode->outAdjList.end();currentNode++){
      cout << (*childNode)->nodeId << endl;
  }
  for(childNode = currentNode->outAdjList.begin(); childNode != currentNode->outAdjList.end();currentNode++){
      printPlanGraph(*childNode);
  }
  
}
//---------------------------------------------------------------------------
/*Plan* Planner::generatePlan2(DataEngine &engine, vector<QueryInfo> &queries)
// Generates a plan for the `queries`.
{
    Plan *p = new Plan();

    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) &engine.relations[0]);
    p->nodes.push_back((AbstractNode *) &engine.relations[5]);

    FilterOperatorNode *filterOne = new FilterOperatorNode(queries[0].filters[0]);
    FilterOperatorNode *filterTwo = new FilterOperatorNode(queries[0].filters[1]);

    filterOne->selectionsInfo.push_back(queries[0].selections[3]);
    filterOne->selectionsInfo.push_back(queries[0].selections[4]);

    filterTwo->selectionsInfo.push_back(queries[0].selections[0]);
    filterTwo->selectionsInfo.push_back(queries[0].selections[1]);
    filterTwo->selectionsInfo.push_back(queries[0].selections[2]);

    p->nodes.push_back((AbstractNode *) filterOne);
    p->nodes.push_back((AbstractNode *) filterTwo);

    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) new DataNode());

    FilterOperatorNode *filterThree = new FilterOperatorNode(queries[0].filters[2]);

    filterThree->selectionsInfo.push_back(queries[0].selections[0]);
    filterThree->selectionsInfo.push_back(queries[0].selections[1]);

    p->nodes.push_back((AbstractNode *) filterThree);

    p->nodes.push_back((AbstractNode *) new DataNode());

    AggregateOperatorNode *aggregateOne = new AggregateOperatorNode();

    aggregateOne->selectionsInfo.push_back(queries[0].selections[3]);

    p->nodes.push_back((AbstractNode *) aggregateOne);

    p->nodes.push_back((AbstractNode *) new DataNode());

    AggregateOperatorNode *aggregateTwo = new AggregateOperatorNode();

    aggregateTwo->selectionsInfo.push_back(queries[0].selections[0]);
    aggregateTwo->selectionsInfo.push_back(queries[0].selections[1]);

    p->nodes.push_back((AbstractNode *) aggregateTwo);

    p->nodes.push_back((AbstractNode *) new DataNode());


    p->nodes[0]->setNodeId(0);
    p->nodes[1]->setNodeId(1);
    p->nodes[2]->setNodeId(2);
    p->nodes[3]->setNodeId(3);
    p->nodes[4]->setNodeId(4);
    p->nodes[5]->setNodeId(5);
    p->nodes[6]->setNodeId(6);
    p->nodes[7]->setNodeId(7);
    p->nodes[8]->setNodeId(8);
    p->nodes[9]->setNodeId(9);
    p->nodes[10]->setNodeId(10);
    p->nodes[11]->setNodeId(11);
    p->nodes[12]->setNodeId(12);

    p->root = p->nodes[0];

    p->exitNodes.push_back((DataNode *) p->nodes[10]);
    p->exitNodes.push_back((DataNode *) p->nodes[12]);

    p->nodes[0]->outAdjList.push_back(p->nodes[1]);
    p->nodes[0]->outAdjList.push_back(p->nodes[2]);
    p->nodes[1]->inAdjList.push_back(p->nodes[0]);
    p->nodes[1]->outAdjList.push_back(p->nodes[3]);
    p->nodes[2]->inAdjList.push_back(p->nodes[0]);
    p->nodes[2]->outAdjList.push_back(p->nodes[4]);
    p->nodes[3]->inAdjList.push_back(p->nodes[1]);
    p->nodes[3]->outAdjList.push_back(p->nodes[5]);
    p->nodes[4]->inAdjList.push_back(p->nodes[2]);
    p->nodes[4]->outAdjList.push_back(p->nodes[6]);
    p->nodes[5]->inAdjList.push_back(p->nodes[3]);
    p->nodes[5]->outAdjList.push_back(p->nodes[9]);
    p->nodes[6]->inAdjList.push_back(p->nodes[4]);
    p->nodes[6]->outAdjList.push_back(p->nodes[7]);
    p->nodes[7]->inAdjList.push_back(p->nodes[6]);
    p->nodes[7]->outAdjList.push_back(p->nodes[8]);
    p->nodes[8]->inAdjList.push_back(p->nodes[7]);
    p->nodes[8]->outAdjList.push_back(p->nodes[11]);
    p->nodes[9]->inAdjList.push_back(p->nodes[5]);
    p->nodes[9]->outAdjList.push_back(p->nodes[10]);

    p->nodes[10]->inAdjList.push_back(p->nodes[9]);

    p->nodes[11]->inAdjList.push_back(p->nodes[8]);
    p->nodes[11]->outAdjList.push_back(p->nodes[12]);

    p->nodes[12]->inAdjList.push_back(p->nodes[11]);

    return p;
}*/
//---------------------------------------------------------------------------
