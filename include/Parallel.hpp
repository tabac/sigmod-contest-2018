#pragma once
#include <cstdint>
#include <tbb/tbb.h>
#include "Mixins.hpp"
//---------------------------------------------------------------------------
class ParallelSum {
    private:
    const uint64_t* my_a;
    uint64_t my_sum;

    public:

    void operator()(const tbb::blocked_range<size_t>& r) {
        const uint64_t *a = my_a;
        uint64_t sum = my_sum;
        size_t end = r.end();

        for(size_t i = r.begin(); i != end; ++i) {
            sum += a[i];
        }

        my_sum = sum;
    }


    void join(const ParallelSum &y) { my_sum += y.my_sum; }

    uint64_t getSum(void) const { return this->my_sum; }

    ParallelSum(ParallelSum& x, tbb::split) : my_a(x.my_a), my_sum(0) { }

    ParallelSum(const uint64_t a[] ) : my_a(a), my_sum(0) { }
};
//---------------------------------------------------------------------------
template <size_t I>
class ParallelPush {
    private:
    const uint64_t *inValues;
    const uint64Pair *indices;
    uint64_t *outValues;

    public:

    void operator()(const tbb::blocked_range<size_t> &range) const {
        const uint64_t *inValuesLoc = this->inValues;
        const uint64Pair *indicesLoc = this->indices;
        uint64_t *outValuesLoc = this->outValues;

        size_t end = range.end();

        for(size_t i = range.begin(); i != end; ++i) {
            outValuesLoc[i] = inValuesLoc[std::get<I>(indicesLoc[i])];
        }
    }

    ParallelPush(const uint64_t *inValues, const uint64Pair *indices, uint64_t *outValues) :
        inValues(inValues), indices(indices), outValues(outValues) { }
};
//---------------------------------------------------------------------------
uint64_t calcParallelSum(IteratorPair valIter);
//---------------------------------------------------------------------------
