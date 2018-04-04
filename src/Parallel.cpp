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
    }
}
//---------------------------------------------------------------------------
