#include <iostream>
#include <vector>
#include "Parser.hpp"
#include "Planner.hpp"
#include "Relation.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
Plan* Planner::generatePlan(DataEngine &engine, vector<QueryInfo> &queries)
// Generates a plan for the `queries`.
{
    Plan *p = new Plan();

    /* Join only */
    // 0 5|0.0=1.0|0.0 1.0 0.1 1.2
    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) &engine.relations[0]);
    p->nodes.push_back((AbstractNode *) &engine.relations[5]);

    JoinOperatorNode *joinOperator = new JoinOperatorNode(queries[0].predicates[0]);

    joinOperator->selectionsInfo.push_back(queries[0].selections[0]);
    joinOperator->selectionsInfo.push_back(queries[0].selections[1]);
    joinOperator->selectionsInfo.push_back(queries[0].selections[2]);
    joinOperator->selectionsInfo.push_back(queries[0].selections[3]);

    p->nodes.push_back((AbstractNode *) joinOperator);

    p->nodes.push_back((AbstractNode *) new DataNode());

    AggregateOperatorNode *aggregateOne = new AggregateOperatorNode();

    aggregateOne->selectionsInfo.push_back(queries[0].selections[0]);
    aggregateOne->selectionsInfo.push_back(queries[0].selections[1]);
    aggregateOne->selectionsInfo.push_back(queries[0].selections[2]);
    aggregateOne->selectionsInfo.push_back(queries[0].selections[3]);

    p->nodes.push_back((AbstractNode *) aggregateOne);

    p->nodes.push_back((AbstractNode *) new DataNode());

    p->nodes[0]->setNodeId(0);
    p->nodes[1]->setNodeId(1);
    p->nodes[2]->setNodeId(2);
    p->nodes[3]->setNodeId(3);
    p->nodes[4]->setNodeId(4);
    p->nodes[5]->setNodeId(5);
    p->nodes[6]->setNodeId(6);

    p->root = p->nodes[0];

    p->exitNodes.push_back((DataNode *) p->nodes[6]);

    p->nodes[0]->outAdjList.push_back(p->nodes[1]);
    p->nodes[0]->outAdjList.push_back(p->nodes[2]);
    p->nodes[1]->inAdjList.push_back(p->nodes[0]);
    p->nodes[1]->outAdjList.push_back(p->nodes[3]);
    p->nodes[2]->inAdjList.push_back(p->nodes[0]);
    p->nodes[2]->outAdjList.push_back(p->nodes[3]);
    p->nodes[3]->inAdjList.push_back(p->nodes[1]);
    p->nodes[3]->inAdjList.push_back(p->nodes[2]);
    p->nodes[3]->outAdjList.push_back(p->nodes[4]);
    p->nodes[4]->inAdjList.push_back(p->nodes[3]);
    p->nodes[4]->outAdjList.push_back(p->nodes[5]);
    p->nodes[5]->inAdjList.push_back(p->nodes[4]);
    p->nodes[5]->outAdjList.push_back(p->nodes[6]);
    p->nodes[6]->inAdjList.push_back(p->nodes[5]);

    /* Filter only
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
    pn->nodes[6]->outAdjList.push_back(p->nodes[7]);
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
    */

    /* Filter + Join

    // 5 0|0.2=1.0&0.3=9881|1.1 0.2 1.0

    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) &engine.relations[5]);
    p->nodes.push_back((AbstractNode *) &engine.relations[0]);

    JoinOperatorNode *joinOperator = new JoinOperatorNode(queries[0].predicates[0]);

    joinOperator->selectionsInfo.push_back(queries[0].selections[0]);
    joinOperator->selectionsInfo.push_back(queries[0].selections[1]);
    joinOperator->selectionsInfo.push_back(queries[0].selections[2]);

    SelectInfo *s = new SelectInfo(5, 0, 3);

    joinOperator->selectionsInfo.push_back(*s);

    p->nodes.push_back((AbstractNode *) joinOperator);

    p->nodes.push_back((AbstractNode *) new DataNode());

    FilterOperatorNode *filterOperator = new FilterOperatorNode(queries[0].filters[0]);

    filterOperator->selectionsInfo.push_back(queries[0].selections[0]);
    filterOperator->selectionsInfo.push_back(queries[0].selections[1]);
    filterOperator->selectionsInfo.push_back(queries[0].selections[2]);

    p->nodes.push_back((AbstractNode *) filterOperator);

    p->nodes.push_back((AbstractNode *) new DataNode());

    AggregateOperatorNode *aggregateOne = new AggregateOperatorNode();

    aggregateOne->selectionsInfo.push_back(queries[0].selections[0]);
    aggregateOne->selectionsInfo.push_back(queries[0].selections[1]);
    aggregateOne->selectionsInfo.push_back(queries[0].selections[2]);

    p->nodes.push_back((AbstractNode *) aggregateOne);

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


    p->root = p->nodes[0];

    p->exitNodes.push_back((DataNode *) p->nodes[8]);

    p->nodes[0]->outAdjList.push_back(p->nodes[1]);
    p->nodes[0]->outAdjList.push_back(p->nodes[2]);

    p->nodes[1]->inAdjList.push_back(p->nodes[0]);
    p->nodes[1]->outAdjList.push_back(p->nodes[3]);

    p->nodes[2]->inAdjList.push_back(p->nodes[0]);
    p->nodes[2]->outAdjList.push_back(p->nodes[3]);

    p->nodes[3]->inAdjList.push_back(p->nodes[1]);
    p->nodes[3]->inAdjList.push_back(p->nodes[2]);
    p->nodes[3]->outAdjList.push_back(p->nodes[4]);

    p->nodes[4]->inAdjList.push_back(p->nodes[3]);
    p->nodes[4]->outAdjList.push_back(p->nodes[5]);

    p->nodes[5]->inAdjList.push_back(p->nodes[4]);
    p->nodes[5]->outAdjList.push_back(p->nodes[6]);

    p->nodes[6]->inAdjList.push_back(p->nodes[5]);
    p->nodes[6]->outAdjList.push_back(p->nodes[7]);

    p->nodes[7]->inAdjList.push_back(p->nodes[6]);
    p->nodes[7]->outAdjList.push_back(p->nodes[8]);

    p->nodes[8]->inAdjList.push_back(p->nodes[7]);
    */

    return p;
}
//---------------------------------------------------------------------------
