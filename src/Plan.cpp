#include <vector>
#include <cstdint>
#include <cassert>
#include <iostream>
#include "Plan.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void ResultInfo::printResultInfo()
// Prints the `results` vector to stdout.
{
    vector<optional<uint64_t>>::iterator it;
    for (it = this->results.begin(); it != this->results.end(); ++it) {
        if ((*it).has_value()) {
            cout << (*it).value() << " ";
        } else {
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
    // TODO: We return by value here, anything better?
    ResultInfo result;

    // Should never be called earlier.
    assert(this->isStatusProcessed());

    // Reserve memory for results.
    result.results.reserve(this->columns.size());

    if (this->size == 0) {
        // Return empty results for each column.
        for (uint64_t i = 0; i < this->columns.size(); ++i) {
            result.results.push_back(optional<uint64_t>());
        }
    } else {
        // Calculate aggregated sum for each column.
        for (uint64_t i = 0; i < this->columns.size(); ++i) {
            uint64_t sum = 0;

            vector<uint64_t>::iterator it;
            for (it = this->dataValues.begin() + i * this->size;
                 it != this->dataValues.begin() + (i + 1) * this->size; ++it) {
                sum += *it;
            }

            result.results.push_back(sum);
        }
    }

    return result;
}
//---------------------------------------------------------------------------
IteratorPair DataNode::getIdsIterator(FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s ids.
{
    // Should not be called with some filter condition.
    assert(filterInfo == NULL);

    return {this->dataIds.begin(), this->dataIds.end()};
}
//---------------------------------------------------------------------------
IteratorPair DataNode::getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s values
// of the column specified by `selectInfo`.
{
    // Should not be called with some filter condition.
    assert(filterInfo == NULL);

    // Find filter column index in `data`
    unsigned c = 0;
    vector<SelectInfo *>::iterator it;
    for (it = this->columns.begin(); it != this->columns.end(); ++it, ++c) {
        if ((*(*it)) == selectInfo) {
            break;
        }
    }

    assert(c < this->columns.size());

    return {
        this->dataValues.begin() + c * this->size,
        this->dataValues.begin() + (c + 1) * this->size,
    };
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
        !this->inAdjList[1]->isStatusProcessed()) {
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
    {
        // Should never be called otherwise.
        assert(this->isStatusFresh());

        // Sould have only one incoming edge.
        assert(this->inAdjList.size() == 1);
        // Sould have only one outgoing edge.
        assert(this->outAdjList.size() == 1);

        // Should not be processed yet.
        assert(this->outAdjList[0]->isStatusFresh());
    }

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
