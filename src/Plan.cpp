#include <vector>
#include <cstdint>
#include <cassert>
#include <iostream>
#include "Plan.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
ResultInfo::ResultInfo(std::vector<uint64_t> results, unsigned size)
// TODO: This should be done better.
{
    this->results.reserve(size);
    if (results.size() != size) {
        assert(results.size() == 0);

        for (uint64_t i = 0; i < size; ++i) {
            this->results.push_back(optional<uint64_t>());
        }
    } else {
        vector<uint64_t>::iterator it;
        for (it = results.begin(); it != results.end(); ++it) {
            this->results.push_back((*it));
        }
    }
}
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

    /*
    cout << "-----------" << endl;
    vector<uint64_t>::iterator jt;
    for (jt = this->dataValues.begin(); jt != this->dataValues.end(); ++jt) {
        cout << (*jt) << " ";
    }
    cout << endl;
    cout << "-----------" << endl;
    */

    // If so set status to `processed`.
    if (allInProcessed) {
        this->setStatus(processed);
    }
}
//---------------------------------------------------------------------------
IteratorPair DataNode::getIdsIterator(SelectInfo& selectInfo, FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s ids.
{
    // Should not be called with some filter condition.
    assert(filterInfo == NULL);
    // Should have at least one column.
    assert(!this->columnsInfo.empty());

    unsigned c = 0;
    RelationId relId = this->columnsInfo[0].relId;

    vector<SelectInfo>::iterator it;
    for (it = this->columnsInfo.begin(); it != this->columnsInfo.end(); ++it, ++c) {
        if ((*it).relId != relId) {
            ++c;
            relId = (*it).relId;
        }

        if ((*it).relId == selectInfo.relId && (*it).binding == selectInfo.binding) {
            break;
        }
    }

    return {
        this->dataIds.begin() + c * this->size,
        this->dataIds.begin() + (c + 1) * this->size,
    };
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
    {
        // Should never be called otherwise.
        assert(this->isStatusFresh());

        // Sould have only two incoming edges.
        assert(this->inAdjList.size() == 2);
        // Sould have only one outgoing edge.
        assert(this->outAdjList.size() == 1);

        // Should not be processed yet.
        assert(this->outAdjList[0]->isStatusFresh());
    }

    // Return if one of the parent nodes has not
    // finished processing.
    if (!this->inAdjList[0]->isStatusProcessed() ||
        !this->inAdjList[1]->isStatusProcessed()) {
        return;
    }

    // Set status to processing.
    this->setStatus(processing);

    cout << "Executing Join: " << this->nodeId << endl;

    // Ugly castings...
    AbstractDataNode *inLeftNode = (AbstractDataNode *) this->inAdjList[0];
    AbstractDataNode *inRightNode = (AbstractDataNode *) this->inAdjList[1];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Get sorted vector<{rowVal, rowId}> for left column.
    IteratorPair idsIter = inLeftNode->getIdsIterator(this->info.left, NULL);
    IteratorPair valIter = inLeftNode->getValuesIterator(this->info.left, NULL);

    vector<pair<uint64_t, uint64_t>> leftPair;
    JoinOperatorNode::getColumnIdPair(idsIter, valIter, leftPair);


    // Get sorted vector<{rowVal, rowId}> for right column.

    // Merge the two vectors and get a vector<{rowId, rowId}>.

    // Get Indices for left by rowId.

    // Add columns to result for left.

    // Get Indices for right by rowId.

    // Add columns to result for right.

    // Set status to processed.
    this->setStatus(processed);
}
//---------------------------------------------------------------------------
void JoinOperatorNode::getColumnIdPair(IteratorPair &ids, IteratorPair &values,
                                       vector<pair<uint64_t, uint64_t>> &pairs)
{
    assert(distance(ids.first, ids.second) == distance(values.first, values.second));

    // TODO: Maybe reserve memory based on the above distance.

    vector<uint64_t>::iterator it, jt;
    for (it = ids.first, jt = values.first; it != ids.second; ++it, ++jt) {
        pairs.push_back({(*jt), (*it)});
    }
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

    // Ugly castings...
    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Get id, values iterators for the filter column.
    IteratorPair idsIter = inNode->getIdsIterator(this->info.filterColumn, NULL);
    IteratorPair valIter = inNode->getValuesIterator(this->info.filterColumn, NULL);

    // Get indices that satisfy the given filter condition.
    vector<uint64_t> indices;
    this->info.getFilteredIndices(valIter, indices);

    // Set the size of the new relation.
    outNode->size = indices.size();

    // Get ouput columns.
    vector<SelectInfo> &selections = this->selectionsInfo;
    if (this->selectionsInfo.empty()) {
        selections = inNode->columnsInfo;
    }

    // Should pass on at least one columns.
    assert(!selections.empty());

    // Reserve memory for ids, column names, column values.
    outNode->dataIds.reserve(outNode->size);
    outNode->columnsInfo.reserve(selections.size());
    outNode->dataValues.reserve(selections.size() * outNode->size);

    unsigned colId = 0, binding = 0;
    vector<SelectInfo>::iterator jt;
    for (jt = selections.begin(); jt != selections.end(); ++jt) {
        // Push column name to new `DataNode`.
        outNode->columnsInfo.emplace_back((*jt));

        // Filter ids.
        vector<uint64_t>::iterator it;
        if (outNode->dataIds.empty() || colId != (*jt).colId || binding != (*jt).binding) {
            idsIter = inNode->getIdsIterator((*jt), NULL);

            for (it = indices.begin(); it != indices.end(); ++it) {
                assert(idsIter.first + (*it) < idsIter.second);

                outNode->dataIds.push_back(*(idsIter.first + (*it)));
            }

            colId = (*jt).colId;
            binding = (*jt).binding;
        }

        // Filter values.
        valIter = inNode->getValuesIterator((*jt), NULL);
        for (it = indices.begin(); it != indices.end(); ++it) {
            assert(valIter.first + (*it) < valIter.second);

            outNode->dataValues.push_back(*(valIter.first + (*it)));
        }
    }

    assert(outNode->dataIds.size() == selections.size() * outNode->size);

    // Set status to processed.
    this->setStatus(processed);
}
//---------------------------------------------------------------------------
void AggregateOperatorNode::execute()
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

        // Should have at least one column.
        assert(this->selectionsInfo.size() > 0);
    }

    // Set status to processing.
    this->setStatus(processing);

    cout << "Executing Aggregate: " << this->nodeId << endl;

    // Ugly castings...
    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Reserve memory for results.
    outNode->dataValues.reserve(this->selectionsInfo.size());

    // Set row count for outNode;
    outNode->size = 1;

    if (inNode->size != 0) {
        // Calculate aggregated sum for each column.
        vector<SelectInfo>::iterator it;
        for (it = this->selectionsInfo.begin(); it != this->selectionsInfo.end(); ++it) {
            outNode->columnsInfo.emplace_back((*it));

            IteratorPair valIter = inNode->getValuesIterator((*it), NULL);

            uint64_t sum = 0;
            vector<uint64_t>::iterator jt;
            for (jt = valIter.first; jt != valIter.second; ++jt) {
                sum += (*jt);
            }

            outNode->dataValues.push_back(sum);
        }

        assert(outNode->columnsInfo.size() == outNode->dataValues.size());
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
