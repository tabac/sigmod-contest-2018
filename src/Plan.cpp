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
void AbstractNode::resetStatus()
// Resets the nodes status, adjacency lists.
{
    this->visited = 0;
    this->status = fresh;

    this->inAdjList.clear();
    this->outAdjList.clear();
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

    vector<uint64_t>::iterator jt;
    for (jt = this->dataValues.begin(); jt != this->dataValues.end(); ++jt) {
        cout << (*jt) << " ";
    }
    cout << endl;

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
    result.results.reserve(this->columnsInfo.size());

    if (this->size == 0) {
        // Return empty results for each column.
        for (uint64_t i = 0; i < this->columnsInfo.size(); ++i) {
            result.results.push_back(optional<uint64_t>());
        }
    } else {
        // Calculate aggregated sum for each column.
        for (uint64_t i = 0; i < this->columnsInfo.size(); ++i) {
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
    vector<SelectInfo>::iterator it;
    for (it = this->columnsInfo.begin(); it != this->columnsInfo.end(); ++it, ++c) {
        if ((*it) == selectInfo) {
            break;
        }
    }

    assert(c < this->columnsInfo.size());

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

    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    IteratorPair idsIter = inNode->getIdsIterator(NULL);
    IteratorPair valIter = inNode->getValuesIterator(this->info.filterColumn, NULL);

    // Get indices that satisfy the given filter condition.
    vector<uint64_t> indices;
    this->info.getFilteredIndices(valIter, indices);

    // Reserve memory for ids, values.
    outNode->dataIds.reserve(indices.size());

    // Filter ids.
    vector<uint64_t>::iterator it;
    for (it = indices.begin(); it != indices.end(); ++it) {
        assert(idsIter.first + (*it) < idsIter.second);
        outNode->dataIds.push_back(*(idsIter.first + (*it)));
    }

    // Reserve memory for column names, column values.
    outNode->columnsInfo.reserve(inNode->columnsInfo.size());
    outNode->dataValues.reserve(inNode->columnsInfo.size() * indices.size());

    // Filter values.
    vector<SelectInfo>::iterator kt;
    for (kt = inNode->columnsInfo.begin(); kt != inNode->columnsInfo.end(); ++kt) {
        outNode->columnsInfo.emplace_back((*kt));

        valIter = inNode->getValuesIterator((*kt), NULL);

        for (it = indices.begin(); it != indices.end(); ++it) {
            assert(valIter.first + (*it) < valIter.second);
            outNode->dataValues.push_back(*(valIter.first + (*it)));
        }
    }

    // Set status to processed.
    this->setStatus(processed);
}
//---------------------------------------------------------------------------
Plan::~Plan()
{
    vector<AbstractNode *>::iterator it;
    for (it = this->nodes.begin(); it != this->nodes.end(); ++it) {
        // Delete intermediate nodes and reset intial relations.
        if (((*it) != this->root) && ((*it)->inAdjList[0] != this->root)) {
            delete (*it);
        } else {
            (*it)->resetStatus();
        }
    }

    delete this->root;
}
//---------------------------------------------------------------------------
