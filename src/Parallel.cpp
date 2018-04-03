#include <cstdint>
#include <cassert>
#include "Parallel.hpp"
#include "Mixins.hpp"
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
//---------------------------------------------------------------------------
void ParallelMerge::mergeJoin(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                              size_t size, size_t begin, size_t end, uint64VecCc &indexPairs) const
{
    size_t l = begin, r = 0;

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
                indexPairs.emplace_back(leftIndex, rightPairs[t].first);
            }

            ++l;
        }
    }
}
//---------------------------------------------------------------------------
