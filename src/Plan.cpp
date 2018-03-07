#include <vector>
#include <variant>
#include <cstdint>
#include <cassert>
#include <iostream>
#include "Plan.hpp"
//---------------------------------------------------------------------------
void ResultInfo::printResultInfo()
// Prints the `results` vector to stdout.
{
    vector<variant<uint64_t, bool>>::iterator it;
    for (it = this->results.begin(); it != this->results.end(); ++it) {

        try {
            cout << get<uint64_t>(*it) << " ";
        } catch (const bad_variant_access&) {
            cout << "NULL ";
        }
    }
    cout << endl;
}
//---------------------------------------------------------------------------
void ResultInfo::printResults(vector<ResultInfo> resultsInfo)
// Prints the `ResultInfo` vector to stdout.
{
    vector<ResultInfo>::iterator it;
    for (it = resultsInfo.begin(); it != resultsInfo.end(); ++it) {
        it->printResultInfo();
    }
    cout << endl;
}
//---------------------------------------------------------------------------
void DataNode::execute()
// Checks if the nodes it depends on are `processed`
// and if so sets its flag to processed too.
{
    // Should never be called otherwise.
    assert(this->isStatusFresh());

    // Sould have only one incoming edge.
    assert(this->inAdjList.size() == 1);
    // Sould have one or zero outgoing edges.
    assert(this->outAdjList.size() < 2);

    if (this->outAdjList.size() == 1) {
        // Should not be processed yet.
        assert(this->outAdjList[0]->isStatusFresh());
    }

    // Check that all parent nodes are processed.
    bool allInProcessed = true;
    vector<AbstractNode *>::iterator it;
    for (it = this->inAdjList.begin(); it != this->inAdjList.end(); ++it) {
        allInProcessed &= (*it)->isStatusProcessed();
    }

    cout << "Executing Data: " << this->nodeId << endl;

    // If so set status to `processed`.
    if (allInProcessed) {
        this->setStatus(processed);
    }
}
//---------------------------------------------------------------------------
ResultInfo DataNode::aggregate()
{
    ResultInfo result;

    return result;
}
//---------------------------------------------------------------------------
void JoinOperatorNode::execute()
// Joins the two input `DataNode` instances.
{
    // Should never be called otherwise.
    assert(this->isStatusFresh());

    // Sould have only two incoming edges.
    assert(this->inAdjList.size() == 2);
    // Sould have only one outgoing edge.
    assert(this->outAdjList.size() == 1);

    // Should not be processed yet.
    assert(this->outAdjList[0]->isStatusFresh());

    // Return if one of the parent nodes has not
    // finished processing.
    if (!this->inAdjList[0]->isStatusProcessed() ||
        !this->inAdjList[0]->isStatusProcessed()) {
        return;
    }

    // Set status to processing.
    this->setStatus(processing);

    cout << "Executing Join: " << this->nodeId << endl;

    // Set status to processed.
    this->setStatus(processed);
}
//---------------------------------------------------------------------------
void FilterOperatorNode::execute()
// Filters the input `DataNode` instance.
{
    // Should never be called otherwise.
    assert(this->isStatusFresh());

    // Sould have only one incoming edge.
    assert(this->inAdjList.size() == 1);
    // Sould have only one outgoing edge.
    assert(this->outAdjList.size() == 1);

    // Should not be processed yet.
    assert(this->outAdjList[0]->isStatusFresh());

    // Set status to processing.
    this->setStatus(processing);

    cout << "Executing Filter: " << this->nodeId << endl;

    // Set status to processed.
    this->setStatus(processed);
}
//---------------------------------------------------------------------------
Plan::~Plan()
{
    vector<AbstractNode *>::iterator it;
    for (it = this->nodes.begin(); it != this->nodes.end(); ++it) {
        delete (*it);
    }
}
//---------------------------------------------------------------------------
