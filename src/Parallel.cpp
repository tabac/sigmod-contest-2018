#include <cstdint>
#include <cassert>
#include "Parallel.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
uint64_t calcParallelSum(IteratorPair valIter)
{
    const uint64_t *values = &(*valIter.first);
    size_t size = valIter.second - valIter.first;

    ParallelSum s(values);

    tbb::parallel_reduce(tbb::blocked_range<size_t>(0, size, SINGLES_GRAIN_SIZE), s);

    return s.getSum();
}
//---------------------------------------------------------------------------
void getValuesIndexedParallel(const IteratorPair valIter, std::vector<uint64Pair> &pairs)
{
    assert(valIter.second - valIter.first > 0);

    const size_t valuesSize = valIter.second - valIter.first;

    if (valuesSize != 0) {
        // This one is important here!
        pairs.resize(valuesSize);

        uint64Pair *pairsPtr = &pairs[0];
        const uint64_t *valuesPtr = &(*valIter.first);

        ParallelIndex i(valuesPtr, pairsPtr);

        tbb::parallel_for(tbb::blocked_range<size_t>(0, valuesSize, PAIRS_GRAIN_SIZE), i);
    }
}
//---------------------------------------------------------------------------
void ParallelMerge::mergeJoin(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                              size_t size, size_t begin, size_t end, uint64VecCc &indexPairs) const
{
    size_t l = begin, r = 0;
    vector<uint64Pair> indexPairsLoc;

    indexPairsLoc.reserve(end - begin);

    while (l < end && r < size) {
        uint64_t left = leftPairs[l].second;
        uint64_t right = rightPairs[r].second;

        if (left < right) {
            ++l;
        } else if (left > right) {
            ++r;
        } else {
            uint64_t leftIndex = leftPairs[l].first;
            for (size_t t = r; t < size && left == rightPairs[t].second; ++t) {
                indexPairsLoc.emplace_back(leftIndex, rightPairs[t].first);
            }

            ++l;
        }
    }

    indexPairs.grow_by(indexPairsLoc.begin(), indexPairsLoc.end());
}
//---------------------------------------------------------------------------
void ParallelMergeR::mergeJoin(size_t begin, size_t end)
{
    size_t l = begin, r = 0, size = this->pairsSize;

    this->indexPairs.reserve(end - begin);

    const uint64Pair *leftPtr = &this->leftPairs[0];
    const uint64Pair *rightPtr = &this->rightPairs[0];

    __builtin_prefetch(&leftPtr[begin], 0, 0);
    __builtin_prefetch(&rightPtr[0], 0, 0);
    while (l < end && r < size) {
        uint64_t left = leftPtr[l].second;
        uint64_t right = rightPtr[r].second;

        if (left < right) {
            ++l;
        } else if (left > right) {
            ++r;
        } else {
            uint64_t leftIndex = leftPtr[l].first;
            for (size_t t = r; t < size && left == rightPtr[t].second; ++t) {
                this->indexPairs.emplace_back(leftIndex, rightPtr[t].first);
            }

            ++l;
        }
    }
}
