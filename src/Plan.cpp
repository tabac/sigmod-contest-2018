#include <vector>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <optional>
#include <algorithm>
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
{
    vector<ResultInfo>::iterator it;
    for (it = resultsInfo.begin(); it != resultsInfo.end(); ++it) {
        it->printResultInfo();
    }
}
//---------------------------------------------------------------------------
void AbstractNode::resetStatus()
{
    this->visited = 0;
    this->status = fresh;

    this->inAdjList.clear();
    this->outAdjList.clear();
}
//---------------------------------------------------------------------------
void AbstractNode::connectNodes(AbstractNode *left, AbstractNode *right)
// TODO: Why this cannot be inline???
{
    left->outAdjList.push_back(right);
    right->inAdjList.push_back(left);
}
//---------------------------------------------------------------------------
void DataNode::execute()
// TODO: Ultimately we just want to update the status here.
{
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
    }

    // Check that all parent nodes are processed.
    bool allInProcessed = true;
    vector<AbstractNode *>::iterator it;
    for (it = this->inAdjList.begin(); it != this->inAdjList.end(); ++it) {
        allInProcessed &= (*it)->isStatusProcessed();
    }

    DEBUGLN("Executing Data");
    // If so set status to `processed`.
    if (allInProcessed) {
        this->setStatus(processed);
    }
}
//---------------------------------------------------------------------------
IteratorPair DataNode::getIdsIterator(SelectInfo& selectInfo, FilterInfo* filterInfo)
// Returns an `IteratorPair` over all the `DataNode`'s ids.
// TODO: We can do without this, without `id` vectors.
{
    {
        // Should not be called with some filter condition.
        assert(filterInfo == NULL);
        // Should have at least one column.
        assert(!this->columnsInfo.empty());
    }

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

    assert(c < this->columnsInfo.size());

    return {
        this->dataIds.begin() + c * this->size,
        this->dataIds.begin() + (c + 1) * this->size,
    };
}
//---------------------------------------------------------------------------
optional<IteratorPair> DataNode::getValuesIterator(SelectInfo& selectInfo,
                                                   FilterInfo* filterInfo)
{
    {
        // Should not be called with some filter condition.
        assert(filterInfo == NULL);
        // Should have at least one column.
        assert(!this->columnsInfo.empty());
    }

    // Find filter column index in `data`
    unsigned c = 0;
    vector<SelectInfo>::iterator it;
    for (it = this->columnsInfo.begin(); it != this->columnsInfo.end(); ++it, ++c) {
        if ((*it) == selectInfo) {
            break;
        }
    }

    if (it == this->columnsInfo.end()) {
        return nullopt;
    } else {
        return optional<IteratorPair>{{
            this->dataValues.begin() + c * this->size,
            this->dataValues.begin() + (c + 1) * this->size,
        }};
    }
}
//---------------------------------------------------------------------------
static bool compare(const uint64Pair &a, const uint64Pair &b)
{
    return a.second < b.second;
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

        // Should specify selections. Simplifies things
        // with bindings...
        assert(this->selectionsInfo.size() != 0);
    }

    // Return if one of the parent nodes has not
    // finished processing.
    if (!this->inAdjList[0]->isStatusProcessed() ||
        !this->inAdjList[1]->isStatusProcessed()) {
        return;
    }

    // Set status to processing.
    this->setStatus(processing);

    DEBUGLN("Executing Join.");

    // Ugly castings...
    AbstractDataNode *inLeftNode = (AbstractDataNode *) this->inAdjList[0];
    AbstractDataNode *inRightNode = (AbstractDataNode *) this->inAdjList[1];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Get sorted vector<{rowIndex, rowValue}> for left column.
    vector<uint64Pair> leftPairs;
    JoinOperatorNode::getValuesIndexedSorted(leftPairs, this->info.left, inLeftNode);

    // Get sorted vector<{rowIndex, rowValue}> for right column.
    vector<uint64Pair> rightPairs;
    JoinOperatorNode::getValuesIndexedSorted(rightPairs, this->info.right, inRightNode);

    // Merge the two vectors and get a pair of vectors:
    // {vector<leftIndices>, vector<rightIndex>}.
    pair<vector<uint64_t>, vector<uint64_t>> indexPairs;
    JoinOperatorNode::mergeJoin(leftPairs, rightPairs, indexPairs);

    assert(indexPairs.first.size() == indexPairs.second.size());

    // Set out DataNode size.
    outNode->size = indexPairs.first.size();

    if (this->selectionsInfo.empty()) {
        // Get ouput columns for left relation and push
        // values to the next `DataNode`.
        JoinOperatorNode::pushSelections(inLeftNode->columnsInfo,
                                         indexPairs.first,
                                         inLeftNode, outNode);

        // Get ouput columns for right relation and push
        // values to the next `DataNode`.
        JoinOperatorNode::pushSelections(inRightNode->columnsInfo,
                                         indexPairs.second,
                                         inRightNode, outNode);
    } else {
        // Get ouput columns for left relation and push
        // values to the next `DataNode`.
        JoinOperatorNode::pushSelections(this->selectionsInfo,
                                         indexPairs.first,
                                         inLeftNode, outNode);

        // Get ouput columns for right relation and push
        // values to the next `DataNode`.
        JoinOperatorNode::pushSelections(this->selectionsInfo,
                                         indexPairs.second,
                                         inRightNode, outNode);
    }

    assert(outNode->dataValues.size() == outNode->columnsInfo.size() * outNode->size);

    // Set status to processed.
    this->setStatus(processed);
}
//---------------------------------------------------------------------------
void JoinOperatorNode::mergeJoin(vector<uint64Pair> &leftPairs,
                                 vector<uint64Pair> &rightPairs,
                                 pair<vector<uint64_t>, vector<uint64_t>> &indexPairs)
{
    vector<uint64Pair>::iterator lt = leftPairs.begin();
    vector<uint64Pair>::iterator rt = rightPairs.begin();

    while (lt != leftPairs.end() && rt != rightPairs.end()) {
        if ((*lt).second < (*rt).second) {
            ++lt;
        } else if ((*lt).second > (*rt).second) {
            ++rt;
        } else {
            vector<uint64Pair>::iterator tt;
            for (tt = rt; tt != rightPairs.end() && (*lt).second == (*tt).second; ++tt) {
                indexPairs.first.push_back((*lt).first);
                indexPairs.second.push_back((*tt).first);
            }

            ++lt;
        }
    }
}
//---------------------------------------------------------------------------
inline void AbstractOperatorNode::pushSelections(vector<SelectInfo> &selections,
                                                 vector<uint64_t> &indices,
                                                 AbstractDataNode *inNode,
                                                 DataNode *outNode)
{
    {
        // Should pass on at least one column.
        assert(!selections.empty());

        // Should be called for allocated objects.
        assert(inNode != NULL && outNode != NULL);
    }

    // TODO: We do not reserve memory here. We should find a way
    //       to reserve memory based on `selections`.

    vector<SelectInfo>::iterator it;
    for (it = selections.begin(); it != selections.end(); ++it) {
        optional<IteratorPair> valIter = inNode->getValuesIterator((*it), NULL);
        if (!valIter.has_value()) {
            // Skip column if not in `inNode->columnsInfo`.
            continue;
        }

        // Push column name to new `DataNode`.
        outNode->columnsInfo.emplace_back((*it));

        // Push values by `indices` to next `DataNode`.
        AbstractOperatorNode::pushValuesByIndex(valIter.value(), indices,
                                                outNode->dataValues);
    }
}
//---------------------------------------------------------------------------
inline void AbstractOperatorNode::pushValuesByIndex(IteratorPair &valIter,
                                                    vector<uint64_t> &indices,
                                                    vector<uint64_t> &outValues)
{
    vector<uint64_t>::iterator it;
    for (it = indices.begin(); it != indices.end(); ++it) {
        assert(valIter.first + (*it) < valIter.second);

        outValues.push_back(*(valIter.first + (*it)));
    }
}
//---------------------------------------------------------------------------
inline void JoinOperatorNode::getValuesIndexedSorted(vector<uint64Pair> &pairs,
                                                     SelectInfo &selection,
                                                     AbstractDataNode* inNode)
{
    // Reserve memory for pairs.
    pairs.reserve(inNode->getSize());

    optional<IteratorPair> option = inNode->getValuesIterator(selection, NULL);

    assert(option.has_value());
    IteratorPair valIter = option.value();

    // Get pairs of the form `{rowIndex, rowValue}`.
    JoinOperatorNode::getValuesIndexed(valIter, pairs);

    // Sort by `rowValue`.
    sort(pairs.begin(), pairs.end(), compare);
}
//---------------------------------------------------------------------------
inline void AbstractOperatorNode::getValuesIndexed(IteratorPair &values,
                                                   vector<uint64Pair> &pairs)
{
    uint64_t i;
    vector<uint64_t>::iterator it;
    for (i = 0, it = values.first; it != values.second; ++it, ++i) {
        pairs.push_back({i, (*it)});
    }
}
//---------------------------------------------------------------------------
void FilterOperatorNode::execute()
// Filters the input `DataNode` instance.
// TODO: Take a closer look here, again!
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

        // Should specify selections. Simplifies things
        // with bindings...
        assert(this->selectionsInfo.size() != 0);
    }

    // Set status to processing.
    this->setStatus(processing);

    DEBUGLN("Executing Filter.");

    // Ugly castings...
    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Get id, values iterators for the filter column.
    optional<IteratorPair> option = inNode->getValuesIterator(this->info.filterColumn, NULL);
    assert(option.has_value());
    IteratorPair valIter = option.value();

    // Get indices that satisfy the given filter condition.
    vector<uint64_t> indices;
    this->info.getFilteredIndices(valIter, indices);

    // Set the size of the new relation.
    outNode->size = indices.size();

    // Reserve memory for ids, column names, column values.
    outNode->columnsInfo.reserve(this->selectionsInfo.size());
    outNode->dataValues.reserve(this->selectionsInfo.size() * outNode->size);

    FilterOperatorNode::pushSelections(this->selectionsInfo, indices, inNode, outNode);

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

    DEBUGLN("Executing Aggregate.");

    // Ugly castings...
    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Reserve memory for results.
    outNode->dataValues.reserve(this->selectionsInfo.size());

    // Set row count for outNode;
    outNode->size = 1;

    // Calculate aggregated sum for each column.
    vector<SelectInfo>::iterator it;
    for (it = this->selectionsInfo.begin(); it != this->selectionsInfo.end(); ++it) {
        // Push column name to new `DataNode`.
        outNode->columnsInfo.emplace_back((*it));

        if (inNode->getSize() != 0) {
            optional<IteratorPair> option = inNode->getValuesIterator((*it), NULL);
            if (!option.has_value()) {
                continue;
            }
            IteratorPair valIter = option.value();

            // Calculate sum for column.
            uint64_t sum = 0;
            vector<uint64_t>::iterator jt;
            for (jt = valIter.first; jt != valIter.second; ++jt) {
                sum += (*jt);
            }

            outNode->dataValues.push_back(sum);
        }
    }

    assert(outNode->dataValues.size() == 0 ||
           outNode->columnsInfo.size() == outNode->dataValues.size());

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
