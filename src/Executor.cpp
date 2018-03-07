#include <queue>
#include <vector>
#include <variant>
#include <cstdint>
#include <cassert>
#include <iostream>
#include "Executor.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Executor::executePlan(Plan &plan, vector<ResultInfo> &resultsInfo)
// Executes a `Plan`.
{
    unsigned counter = 0, mark = 0;
    queue<AbstractNode *> q;

    // The `root` is a dummy not, set its status to processed.
    plan.root->setStatus(processed);

    do {
        // TODO: This should wait on a condition variable and not
        //       spin on this `counter`.
        counter = 0;

        plan.root->visited = mark;
        q.push(plan.root);

        while (!q.empty()) {
            AbstractNode *cur = q.front();
            q.pop();

            if (cur->isStatusFresh()) {
                Executor::executeOperator(cur);
            } else if (cur->isStatusProcessed()) {
                vector<AbstractNode *>::iterator it;
                for (it = cur->outAdjList.begin(); it != cur->outAdjList.end(); ++it) {
                    if ((*it)->visited != mark) {
                        (*it)->visited = mark;
                        q.push(*it);
                    }
                }

                ++counter;
            }
        }

        ++mark;
    } while (counter != plan.nodes.size());

    assert(resultsInfo.size() == 0);

    vector<DataNode *>::iterator it;
    for (it = plan.exitNodes.begin(); it != plan.exitNodes.end(); ++it) {
        resultsInfo.push_back((*it)->aggregate());
    }
}
//---------------------------------------------------------------------------
void Executor::executeOperator(AbstractNode *node)
// Executes the operator of an `AbstractOperatorNode`.
{
    node->execute();
}
//---------------------------------------------------------------------------
