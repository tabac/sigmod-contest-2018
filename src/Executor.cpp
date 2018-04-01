#include <queue>
#include <future>
#include <vector>
#include <variant>
#include <cstdint>
#include <cassert>
#include <iostream>
#include "Executor.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
vector<thread> threads;
//---------------------------------------------------------------------------
void Executor::executePlan(Plan &plan, vector<ResultInfo> &resultsInfo)
// Executes a `Plan`.
// TODO: This should clear vectors of intermediate nodes if
//       they are not used anymore.
// TODO: This should wait on a condition variable and not
//       spin on this `counter`.
{
    unsigned counter = 0, mark = 0;
    queue<AbstractNode *> q;

    // The `root` is a dummy not, set its status to processed.
    plan.root->setStatus(processed);

    do {
        counter = 0;

        plan.root->visited = mark;
        q.push(plan.root);

        while (!q.empty()) {
            AbstractNode *cur = q.front();
            q.pop();

            if (cur->isStatusFresh()) {
                Executor::executeOperator(cur);
            } else if (cur->isStatusProcessed()) {
                unsigned processed = 0;
                vector<AbstractNode *>::iterator it;
                for (it = cur->outAdjList.begin(); it != cur->outAdjList.end(); ++it) {
                    if ((*it)->visited != mark) {
                        (*it)->visited = mark;
                        q.push(*it);
                    }
                    if ((*it)->isStatusProcessed()) {
                        ++processed;
                    }
                }

                // Call `freeResources()` for each intermediate node
                // that his children are processed.
                // TODO: This gets called too many times. Maybe we
                //       should move the root forward or something.

//                if (processed != 0 && cur->outAdjList.size() == processed) {
//                    cur->freeResources();
//                }

                ++counter;
            }
        }

        ++mark;
    } while (counter != plan.nodes.size());

    assert(resultsInfo.size() == 0);

    vector<thread>::iterator tt;
    for (tt = threads.begin(); tt != threads.end(); ++tt) {
        if (tt->joinable()) {
            tt->join();
        }
    }
    threads.clear();

    vector<DataNode *>::iterator it;
    for (it = plan.exitNodes.begin(); it != plan.exitNodes.end(); ++it) {
        resultsInfo.emplace_back((*it)->dataValues, (*it)->columnsInfo.size());
    }
}
//---------------------------------------------------------------------------
void Executor::executeOperator(AbstractNode *node)
// Executes the operator of an `AbstractOperatorNode`.
{
    // Sequential version:
    node->execute(threads);

    // Async version, using async:
    // async(launch::async, &AbstractNode::execute, node);

    // Async version, using threads:

    // Set status to processing.

    // Create thread and run `execute`.
    // threads.emplace_back(&AbstractNode::execute, node);
}
//---------------------------------------------------------------------------
