#include <thread>
#include <vector>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <optional>
#include <algorithm>
#include <tbb/parallel_sort.h>
#include "Plan.hpp"
#include "Mixins.hpp"
#include "Index.hpp"
#include "Relation.hpp"
#include "Executor.hpp"
#include "Parallel.hpp"
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
void ResultInfo::printResultInfo() const
{
    vector<optional<uint64_t>>::const_iterator it;
    for (it = this->results.begin(); it != this->results.end() - 1; ++it) {
        if ((*it).has_value()) {
            cout << (*it).value() << " ";
        } else {
            cout << "NULL ";
        }
    }

    if ((*it).has_value()) {
        cout << (*it).value();
    } else {
        cout << "NULL";
    }
    cout << endl;
}
//---------------------------------------------------------------------------
void ResultInfo::printResults(const vector<ResultInfo> resultsInfo)
{
    vector<ResultInfo>::const_iterator it;
    for (it = resultsInfo.begin(); it != resultsInfo.end(); ++it) {
        it->printResultInfo();
    }
}
//---------------------------------------------------------------------------
void AbstractNode::resetStatus()
{
    this->visited = 0;
    this->status = NodeStatus::fresh;

    this->inAdjList.clear();
    this->outAdjList.clear();
}
//---------------------------------------------------------------------------
void AbstractNode::connectNodes(AbstractNode *left, AbstractNode *right)
{
    left->outAdjList.push_back(right);
    right->inAdjList.push_back(left);
}
//---------------------------------------------------------------------------
void DataNode::execute(vector<thread> &)
{
    {
        // Should never be called otherwise.
        assert(this->isStatusFresh());

        // Sould have only one incoming edge.
        assert(this->inAdjList.size() == 1);
        // Sould have one or zero outgoing edges.
        // assert(this->outAdjList.size() < 2);

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

#ifndef NDEBUG
    DEBUGLN("Executing Data" + this->label);

    cerr << "ColumnsInfo:" << endl;
    for (auto c : this->columnsInfo) {
        cerr << c.dumpLabel() << " ";
    }
    cerr << endl;
#endif

    // If so set status to `processed`.
    if (allInProcessed) {
        this->setStatus(processed);

        Executor::notify();
    }
}
//---------------------------------------------------------------------------
optional<IteratorPair> DataNode::getIdsIterator(const SelectInfo& , const FilterInfo* )
// Returns `nullopt` for a `DataNode`. The ids are the indices
// in the case of a column.
{
    return nullopt;
}
//---------------------------------------------------------------------------
optional<IteratorPair> DataNode::getValuesIterator(const SelectInfo& selectInfo,
                                                   const FilterInfo* filterInfo)
{
    {
        // Should not be called with some filter condition.
        assert(filterInfo == NULL);
        // Should have at least one column.
        assert(!this->columnsInfo.empty());
    }

    // Find filter column index in `data`
    unsigned c = 0;
    vector<SelectInfo>::const_iterator it;
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
void JoinOperatorNode::execute(vector<thread> &threads)
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
        assert(this->selections.size() != 0);
    }

    // Return if one of the parent nodes has not
    // finished processing.
    if (!this->inAdjList[0]->isStatusProcessed() ||
        !this->inAdjList[1]->isStatusProcessed()) {
        return;
    }

    // Set status to processing.
    this->setStatus(processing);

    threads.emplace_back(&JoinOperatorNode::executeAsync, this);
}
//---------------------------------------------------------------------------
void JoinOperatorNode::executeAsync(void)
{

#ifndef NDEBUG
    DEBUGLN("Executing Join." + this->label);

    cerr << "Selections:" << endl;
    for (auto c : this->selections) {
        cerr << c.dumpLabel() << " ";
    }
    cerr << endl;
#endif

    // Ugly castings...
    AbstractDataNode *inLeftNode = (AbstractDataNode *) this->inAdjList[0];
    AbstractDataNode *inRightNode = (AbstractDataNode *) this->inAdjList[1];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Merge the two vectors and get a pair of vectors:
    // {vector<leftIndices>, vector<rightIndex>}.
    vector<uint64Pair> indexPairs;
    JoinOperatorNode::mergeJoinSeq(this->info.left, this->info.right,
                                   inLeftNode, inRightNode, indexPairs);

    /*
    JoinOperatorNode::hashJoinSeq(this->info.left, this->info.right,
                                 inLeftNode, inRightNode, indexPairs);
    */

    // Set out DataNode size.
    outNode->size = indexPairs.size();

    if (inLeftNode->isBaseRelation()) {
        // Get ouput columns for right relation and push
        // values to the next `DataNode`.
        AbstractOperatorNode::pushSelections<1>(this->selections,
                                             indexPairs,
                                             inRightNode, outNode);
        // Get ouput columns for left relation and push
        // values to the next `DataNode`.
        AbstractOperatorNode::pushSelections<0>(this->selections,
                                             indexPairs,
                                             inLeftNode, outNode);
    } else {
        // Get ouput columns for left relation and push
        // values to the next `DataNode`.
        AbstractOperatorNode::pushSelections<0>(this->selections,
                                             indexPairs,
                                             inLeftNode, outNode);
        // Get ouput columns for right relation and push
        // values to the next `DataNode`.
        AbstractOperatorNode::pushSelections<1>(this->selections,
                                             indexPairs,
                                             inRightNode, outNode);
    }

    // TODO: This is because sometimes the selections have duplicates.
    //       We should fix `setQuerySelections` and remove this.
    outNode->dataValues.resize(outNode->columnsInfo.size() * outNode->size);

    assert(outNode->dataValues.size() == outNode->columnsInfo.size() * outNode->size);

    // Set status to processed.
    this->setStatus(processed);

    Executor::notify();
}
//---------------------------------------------------------------------------
void JoinOperatorNode::mergeJoinSeq(const SelectInfo &left, const SelectInfo &right,
                                    AbstractDataNode *leftNode, AbstractDataNode *rightNode,
                                    vector<uint64Pair> &indexPairs)
{
    // Get sorted vector<{rowIndex, rowValue}> for left column.
    pair<bool, vector<uint64Pair>*> leftPairsOption;
    leftPairsOption = JoinOperatorNode::getValuesIndexedSorted(left, leftNode);

    // Get sorted vector<{rowIndex, rowValue}> for right column.
    pair<bool, vector<uint64Pair>*> rightPairsOption;
    rightPairsOption = JoinOperatorNode::getValuesIndexedSorted(right, rightNode);

    // Keep the smaller column on the left.
    bool swapPairs = leftPairsOption.second->size() > rightPairsOption.second->size();
    if (swapPairs) {
        swap(leftPairsOption, rightPairsOption);
    }

    vector<uint64Pair> &leftPairs = *leftPairsOption.second;
    vector<uint64Pair> &rightPairs = *rightPairsOption.second;

    vector<uint64Pair>::const_iterator lt = leftPairs.begin();
    vector<uint64Pair>::const_iterator rt = rightPairs.begin();

    // TODO: Rewrite this in a more cache friendly way. Loop left, then right.
    //       Look at other branches for solution.
    __builtin_prefetch(&leftPairs[0], 0, 0);
    __builtin_prefetch(&rightPairs[0], 0, 0);

    if (!swapPairs) {
        while (lt != leftPairs.end() && rt != rightPairs.end()) {
            if ((*lt).second < (*rt).second) {
                ++lt;
            } else if ((*lt).second > (*rt).second) {
                ++rt;
            } else {
                vector<uint64Pair>::const_iterator tt;
                for (tt = rt; tt != rightPairs.end() && (*lt).second == (*tt).second; ++tt) {
                    indexPairs.emplace_back(lt->first, tt->first);
                }

                ++lt;
            }
        }
    } else {
        while (lt != leftPairs.end() && rt != rightPairs.end()) {
            if ((*lt).second < (*rt).second) {
                ++lt;
            } else if ((*lt).second > (*rt).second) {
                ++rt;
            } else {
                vector<uint64Pair>::const_iterator tt;
                for (tt = rt; tt != rightPairs.end() && (*lt).second == (*tt).second; ++tt) {
                    indexPairs.emplace_back(tt->first, lt->first);
                }

                ++lt;
            }
        }
    }

    // Free pairs memory if it's owned by them (not an index).
    if (leftPairsOption.first) {
        delete leftPairsOption.second;
    }
    if (rightPairsOption.first) {
        delete rightPairsOption.second;
    }
}
//---------------------------------------------------------------------------
void JoinOperatorNode::hashJoinSeq(const SelectInfo &left, const SelectInfo &right,
                                   AbstractDataNode *leftNode, AbstractDataNode *rightNode,
                                   vector<uint64Pair> &indexPairs)
{
    vector<uint64Pair> leftPairs, rightPairs;

    JoinOperatorNode::getValuesIndexed(left, leftNode, leftPairs);

    JoinOperatorNode::getValuesIndexed(right, rightNode, rightPairs);

    // Keep the smaller column on the left.
    bool swapPairs = leftPairs.size() > rightPairs.size();
    if (swapPairs) {
        swap(leftPairs, rightPairs);
    }

    unordered_map<uint64_t, vector<uint64_t>> map;
    map.reserve(leftPairs.size());

    // Hash-Join: Build Face.
    vector<uint64Pair>::const_iterator it;
    for (it = leftPairs.begin(); it != leftPairs.end(); ++it) {
        map[it->second].push_back(it->first);
    }

    // Hash-Join: Probe Face.
    if (!swapPairs) {
        for (it = rightPairs.begin(); it != rightPairs.end(); ++it) {
            try {
                const vector<uint64_t> &bucket = map.at(it->second);

                vector<uint64_t>::const_iterator jt;
                for (jt = bucket.begin(); jt != bucket.end(); ++jt) {
                    indexPairs.emplace_back((*jt), it->first);
                }
            }
            catch (const out_of_range& oor) {
                continue;
            }
        }
    } else {
        for (it = rightPairs.begin(); it != rightPairs.end(); ++it) {
            try {
                const vector<uint64_t> &bucket = map.at(it->second);

                vector<uint64_t>::const_iterator jt;
                for (jt = bucket.begin(); jt != bucket.end(); ++jt) {
                    indexPairs.emplace_back(it->first, (*jt));
                }
            }
            catch (const out_of_range& oor) {
                continue;
            }
        }
    }
}
//---------------------------------------------------------------------------
void JoinOperatorNode::hashJoinPar(const SelectInfo &left, const SelectInfo &right,
                                   AbstractDataNode *leftNode, AbstractDataNode *rightNode,
                                   vector<uint64Pair> &indexPairs)
{
    vector<uint64Pair> leftPairs, rightPairs;

    JoinOperatorNode::getValuesIndexed(left, leftNode, leftPairs);

    JoinOperatorNode::getValuesIndexed(right, rightNode, rightPairs);

    // TODO: Move to `tbb:concurrent_hash_map`.
    unordered_map<uint64_t, vector<uint64_t>> map;
    map.reserve(leftPairs.size() * 2);

    // TODO: Parallel implementation inserting into the map above.
    // Hash-Join: Build Face.
    vector<uint64Pair>::const_iterator it;
    for (it = leftPairs.begin(); it != leftPairs.end(); ++it) {
        map[it->second].push_back(it->first);
    }

    // TODO: Parallel implementation reading the map above and
    //       storing to a `concurrent_vector`.
    // Hash-Join: Probe Face.
    for (it = rightPairs.begin(); it != rightPairs.end(); ++it) {
        try {
            const vector<uint64_t> &bucket = map.at(it->second);

            vector<uint64_t>::const_iterator jt;
            for (jt = bucket.begin(); jt != bucket.end(); ++jt) {
                indexPairs.emplace_back((*jt), it->first);
            }
        }
        catch (const out_of_range& oor) {
            continue;
        }
    }
}
//---------------------------------------------------------------------------
template <size_t I>
void AbstractOperatorNode::pushSelections(const vector<SelectInfo> &selections,
                                          const vector<uint64Pair> &indices,
                                          AbstractDataNode *inNode,
                                          DataNode *outNode)
{
    {
        // Should pass on at least one column.
        assert(!selections.empty());

        // Should be called for allocated objects.
        assert(inNode != NULL && outNode != NULL);
    }

    outNode->columnsInfo.reserve(selections.size());
    outNode->dataValues.resize(selections.size() * indices.size());

    vector<SelectInfo>::const_iterator it;
    for (it = selections.begin(); it != selections.end(); ++it) {
        // Skip columns already inserted.
        vector<SelectInfo>::const_iterator jt = find(
            outNode->columnsInfo.begin(), outNode->columnsInfo.end(), (*it));
        if (jt != outNode->columnsInfo.end()) {
            continue;
        }

        optional<IteratorPair> option = inNode->getValuesIterator((*it), NULL);
        if (!option.has_value()) {
            // Skip column if not in `inNode->columnsInfo`.
            continue;
        }
        const IteratorPair valIter = option.value();

        // Push column name to new `DataNode`.
        outNode->columnsInfo.emplace_back((*it));

        // Push values by `indices` to next `DataNode`.
        AbstractOperatorNode::pushValuesByIndex<I>(valIter, indices, outNode);
    }
}
//---------------------------------------------------------------------------
template <size_t I>
void AbstractOperatorNode::pushValuesByIndex(const IteratorPair &valIter,
                                             const vector<uint64Pair> &indices,
                                             DataNode *outNode)
{
    const uint64_t *inValuesPtr = &(*valIter.first);
    const uint64Pair *indicesPtr = &indices[0];

    size_t outValuesOffset = (outNode->columnsInfo.size() - 1) * indices.size();
    uint64_t *outValuesPtr = &outNode->dataValues[outValuesOffset];

    ParallelPush<I> p(inValuesPtr, indicesPtr, outValuesPtr);

    tbb::parallel_for(tbb::blocked_range<size_t>(0, indices.size(), PAIRS_GRAIN_SIZE), p);
}
//---------------------------------------------------------------------------
pair<bool, vector<uint64Pair>*> JoinOperatorNode::getValuesIndexedSorted(
    const SelectInfo &selection, AbstractDataNode* inNode)
{
    SortedIndex *index = inNode->getIndex(selection);
    if (INDEXES_ON && index != NULL) {
        return {false, index->getValuesIndexedSorted()};
    } else {
        if (INDEXES_ON && INDEXES_CREATE_ON_MERGE && inNode->isBaseRelation()) {
            // Ugly mother coming up...
            Relation *relation = (Relation *) inNode;

            relation->createIndex(selection);

            return JoinOperatorNode::getValuesIndexedSorted(selection, inNode);
        } else {
            vector<uint64Pair> *pairs = new vector<uint64Pair>();

            JoinOperatorNode::getValuesIndexed(selection, inNode, *pairs);

            if (!pairs->empty()) {
                // Sort by `rowValue`.
                tbb::parallel_sort(pairs->begin(), pairs->end(),
                     [&](const uint64Pair &a, const uint64Pair &b) { return a.second < b.second; });
            }

            return {true, pairs};
        }
    }
}
//---------------------------------------------------------------------------
void JoinOperatorNode::getValuesIndexed(const SelectInfo &selection,
                                        AbstractDataNode *inNode,
                                        vector<uint64Pair> &pairs)
{
    optional<IteratorPair> option = inNode->getValuesIterator(selection, NULL);

    assert(option.has_value());
    const IteratorPair valIter = option.value();

    if (valIter.second - valIter.first != 0) {
        // Reserve memory for pairs.
        pairs.reserve(valIter.second - valIter.first);

        // Get pairs of the form `{rowIndex, rowValue}`.
        getValuesIndexedParallel(valIter, pairs);
    }
}
//---------------------------------------------------------------------------
void FilterOperatorNode::execute(vector<thread> &threads)
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

        // Should specify selections. Simplifies things
        // with bindings...
        assert(this->selections.size() != 0);
    }

    // Set status to processing.
    this->setStatus(processing);

    threads.emplace_back(&FilterOperatorNode::executeAsync, this);
}

void FilterOperatorNode::executeAsync(void)
{

#ifndef NDEBUG
    DEBUGLN("Executing Filter." + this->label);

    cerr << "Selections:" << endl;
    for (auto c : this->selections) {
        cerr << c.dumpLabel() << " ";
    }
    cerr << endl;
#endif

    // Ugly castings...
    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    vector<uint64Pair> indices;
    optional<IteratorPair> idsOption;
    SortedIndex *index = inNode->getIndex(this->info.filterColumn);
    if (INDEXES_ON && index != NULL) {
        // Get {ids, values} iterator for the filter column.
        optional<IteratorDoublePair> option = index->getIdsValuesIterator(
            this->info.filterColumn, &this->info);

        assert(option.has_value());
        IteratorDoublePair idValIter = option.value();

        // Reserve memory for indices.
        indices.reserve(idValIter.second -  idValIter.first);

        // Get indices that satisfy the given filter condition.
        this->info.getFilteredIndices(idValIter, indices);
    } else {
        // Get values iterator for the filter column.
        optional<IteratorPair> option = inNode->getValuesIterator(this->info.filterColumn,
                                                                  NULL);
        assert(option.has_value());
        IteratorPair valIter = option.value();

        // Get ids iterator for the filter column.
        idsOption = inNode->getIdsIterator(this->info.filterColumn, NULL);

        // TODO: Think of something better.
        indices.reserve((valIter.second - valIter.first) / 2);

        // Get indices that satisfy the given filter condition.
        this->info.getFilteredIndices(valIter, idsOption, indices);
    }

    // Reserve memory for ids, column names, column values.
    outNode->columnsInfo.reserve(this->selections.size());
    outNode->dataValues.reserve(this->selections.size() * indices.size());

    // Set the size of the new relation.
    outNode->size = indices.size();

    AbstractOperatorNode::pushSelections<0>(this->selections, indices, inNode, outNode);

    // Set status to processed.
    this->setStatus(processed);

    Executor::notify();
}
//---------------------------------------------------------------------------
void FilterJoinOperatorNode::execute(vector<thread> &threads)
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

        // Should specify selections. Simplifies things
        // with bindings...
        assert(!this->selections.empty());
    }

    // Set status to processing.
    this->setStatus(processing);

    threads.emplace_back(&FilterJoinOperatorNode::executeAsync, this);
}

void FilterJoinOperatorNode::executeAsync(void)
{

#ifndef NDEBUG
    DEBUGLN("Executing Join Filter." + this->label);

    cerr << "Selections:" << endl;
    for (auto c : this->selections) {
        cerr << c.dumpLabel() << " ";
    }
    cerr << endl;
#endif

    // Ugly castings...
    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Get values iterator for the left column.
    optional<IteratorPair> option = inNode->getValuesIterator(this->info.left, NULL);
    assert(option.has_value());
    const IteratorPair leftIter = option.value();

    // Get values iterator for the right column.
    option = inNode->getValuesIterator(this->info.right, NULL);
    assert(option.has_value());
    const IteratorPair rightIter = option.value();

    uint64_t i;
    vector<uint64Pair> indices;
    vector<uint64_t>::const_iterator it, jt;
    for (i = 0, it = leftIter.first, jt = rightIter.first; it != leftIter.second; ++i, ++it, ++jt) {
        if ((*it) == (*jt)) {
            indices.push_back({i, 0});
        }
    }

    assert(jt == rightIter.second);

    // Set the size of the new relation.
    outNode->size = indices.size();

    // Reserve memory for ids, column names, column values.
    outNode->columnsInfo.reserve(this->selections.size());
    outNode->dataValues.reserve(this->selections.size() * outNode->size);

    AbstractOperatorNode::pushSelections<0>(this->selections, indices, inNode, outNode);

    // Set status to processed.
    this->setStatus(processed);

    Executor::notify();
}
//---------------------------------------------------------------------------
void AggregateOperatorNode::execute(vector<thread> &threads)
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
        assert(this->selections.size() > 0);
    }

    // Set status to processing.
    this->setStatus(processing);

    threads.emplace_back(&AggregateOperatorNode::executeAsync, this);
}

void AggregateOperatorNode::executeAsync(void)
{

#ifndef NDEBUG
    DEBUGLN("Executing Aggregate." + this->label);

    cerr << "Selections:" << endl;
    for (auto c : this->selections) {
        cerr << c.dumpLabel() << " ";
    }
    cerr << endl;
#endif

    // Ugly castings...
    AbstractDataNode *inNode = (AbstractDataNode *) this->inAdjList[0];
    DataNode *outNode = (DataNode *) this->outAdjList[0];

    // Reserve memory for results.
    outNode->columnsInfo.reserve(this->selections.size());
    outNode->dataValues.reserve(this->selections.size());

    // Set row count for outNode;
    outNode->size = 1;

    // Calculate aggregated sum for each column.
    vector<SelectInfo>::iterator it;
    for (it = this->selections.begin(); it != this->selections.end(); ++it) {
        // Push column name to new `DataNode`.
        outNode->columnsInfo.emplace_back((*it));

        if (inNode->getSize() != 0) {
            optional<IteratorPair> option = inNode->getValuesIterator((*it), NULL);
            if (!option.has_value()) {
                continue;
            }
            IteratorPair valIter = option.value();

            // Calculate sum for column.
            uint64_t sum = calcParallelSum(valIter);
            outNode->dataValues.push_back(sum);
        }
    }

    assert(outNode->dataValues.size() == 0 ||
           outNode->columnsInfo.size() == outNode->dataValues.size());

    // Set status to processed.
    this->setStatus(processed);

    Executor::notify();
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
