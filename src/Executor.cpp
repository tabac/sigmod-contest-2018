#include <queue>
#include <future>
#include <vector>
#include <cstdint>
#include <cassert>
#include <iostream>
#include "Executor.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
SyncPair Executor::syncPair;
bool Executor::syncFlag = false;
vector<thread> Executor::threads;
//---------------------------------------------------------------------------
void Executor::executePlan(Plan &plan, vector<ResultInfo> &resultsInfo)
// Executes a `Plan`.
{
    unsigned counter = 0, mark = 666;
    queue<AbstractNode *> q;

    // TODO: Proper reservations here.
    Executor::threads.reserve(plan.nodes.size());

    // The `root` is a dummy not, set its status to processed.
    plan.root->setStatus(processed);

    for ( ;; ) {
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
                if (processed != 0 && cur->outAdjList.size() == processed) {
                    cur->freeResources();
                }

                ++counter;
            }
        }

        ++mark;

        if (counter == plan.nodes.size()) {
            break;
        } else {
            Executor::wait();
        }
    }

    assert(resultsInfo.size() == 0);

    vector<thread>::iterator tt;
    for (tt = Executor::threads.begin(); tt != Executor::threads.end(); ++tt) {
        if (tt->joinable()) {
            tt->join();
        }
    }
    Executor::threads.clear();

    vector<DataNode *>::iterator it;
    for (it = plan.exitNodes.begin(); it != plan.exitNodes.end(); ++it) {
        resultsInfo.emplace_back((*it)->dataValues, (*it)->columnsInfo.size());
    }
}
//---------------------------------------------------------------------------
void Executor::executeOperator(AbstractNode *node)
// Executes the operator of an `AbstractOperatorNode`.
{
    node->execute(Executor::threads);
}
//---------------------------------------------------------------------------
void Executor::notify(void)
{
    unique_lock<mutex> lck(Executor::syncPair.first);

    Executor::setFlag();

    lck.unlock();

    Executor::syncPair.second.notify_one();
}
//---------------------------------------------------------------------------
void Executor::wait(void)
{
    unique_lock<mutex> lck(Executor::syncPair.first);

    while (!Executor::syncFlag) {
        Executor::syncPair.second.wait(lck);
    }

    Executor::unsetFlag();

    lck.unlock();
}
//---------------------------------------------------------------------------
