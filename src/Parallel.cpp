#include <cstdint>
#include "Parallel.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
uint64_t calcParallelSum(IteratorPair valIter)
{
    const uint64_t *values = &(*valIter.first);
    size_t size = valIter.second - valIter.first;

    ParrallelSum s(values);

    tbb::parallel_reduce(tbb::blocked_range<size_t>(0, size), s);

    return s.getSum();
}
//---------------------------------------------------------------------------
