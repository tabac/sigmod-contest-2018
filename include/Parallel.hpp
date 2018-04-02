#pragma once
#include <cstdint>
#include <tbb/tbb.h>
#include "Mixins.hpp"
//---------------------------------------------------------------------------
class ParrallelSum {
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


    void join(const ParrallelSum &y) { my_sum += y.my_sum; }

    uint64_t getSum(void) const { return this->my_sum; }

    ParrallelSum(ParrallelSum& x, tbb::split) : my_a(x.my_a), my_sum(0) { }

    ParrallelSum(const uint64_t a[] ) : my_a(a), my_sum(0) { }
};
//---------------------------------------------------------------------------
uint64_t calcParallelSum(IteratorPair valIter);
//---------------------------------------------------------------------------
