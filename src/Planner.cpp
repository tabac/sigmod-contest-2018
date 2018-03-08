#include <vector>
#include "Parser.hpp"
#include "Planner.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
Plan* Planner::generatePlan(DataEngine &engine, vector<QueryInfo> &queries)
// Generates a plan for the `queries`.
{
    Plan *p = new Plan();

    struct PredicateInfo predicate = queries[0].predicates[0];
    struct FilterInfo filter = queries[0].filters[0];

    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) new DataNode());
    p->nodes.push_back((AbstractNode *) new DataNode());

    p->nodes.push_back((AbstractNode *) new JoinOperatorNode(predicate));
    p->nodes.push_back((AbstractNode *) new FilterOperatorNode(filter));

    p->nodes[0]->setNodeId(0);
    p->nodes[1]->setNodeId(1);
    p->nodes[2]->setNodeId(2);
    p->nodes[3]->setNodeId(3);
    p->nodes[4]->setNodeId(4);
    p->nodes[5]->setNodeId(5);
    p->nodes[6]->setNodeId(6);

    p->root = p->nodes[0];

    p->nodes[0]->outAdjList.push_back(p->nodes[1]);
    p->nodes[0]->outAdjList.push_back(p->nodes[2]);

    p->nodes[1]->inAdjList.push_back(p->nodes[0]);
    p->nodes[1]->outAdjList.push_back(p->nodes[6]);

    p->nodes[2]->inAdjList.push_back(p->nodes[0]);
    p->nodes[2]->outAdjList.push_back(p->nodes[5]);

    p->nodes[3]->inAdjList.push_back(p->nodes[5]);

    p->nodes[4]->inAdjList.push_back(p->nodes[6]);
    p->nodes[4]->outAdjList.push_back(p->nodes[5]);

    p->nodes[5]->inAdjList.push_back(p->nodes[2]);
    p->nodes[5]->inAdjList.push_back(p->nodes[4]);
    p->nodes[5]->outAdjList.push_back(p->nodes[3]);

    p->nodes[6]->inAdjList.push_back(p->nodes[1]);
    p->nodes[6]->outAdjList.push_back(p->nodes[4]);

    return p;
}
//---------------------------------------------------------------------------
