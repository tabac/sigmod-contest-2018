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

        __builtin_prefetch(&a[r.begin()], 0, 0);
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
template <size_t I, typename T>
class ParallelPush {
    private:
    const uint64_t *inValues;
    const T &indices;
    uint64_t *outValues;

    public:
    void operator()(const tbb::blocked_range<size_t> &range) const {
        const uint64_t *inValuesLoc = this->inValues;
        uint64_t *outValuesLoc = this->outValues;

        size_t end = range.end();

        __builtin_prefetch(&inValuesLoc[range.begin()], 0, 0);
        __builtin_prefetch(&outValuesLoc[range.begin()], 1, 0);
        for(size_t i = range.begin(); i != end; ++i) {
            outValuesLoc[i] = inValuesLoc[std::get<I>(indices[i])];
        }
    }

    ParallelPush(const uint64_t *inValues, const T &indices, uint64_t *outValues) :
        inValues(inValues), indices(indices), outValues(outValues) { }
};
//---------------------------------------------------------------------------
class ParallelIndex {
    private:
    const uint64_t* values;
    uint64Pair *pairs;

    public:
    void operator()(const tbb::blocked_range<size_t>& range) const {
        const uint64_t *values = this->values;
        uint64Pair *pairs = this->pairs;

        size_t end = range.end();

        __builtin_prefetch(&values[range.begin()], 0, 0);
        __builtin_prefetch(&pairs[range.begin()], 1, 0);
        for(size_t i = range.begin(); i != end; ++i) {
            pairs[i] = {i, values[i]};
        }
    }

    ParallelIndex(const uint64_t *values, uint64Pair *pairs) :
        values(values), pairs(pairs) { }
//---------------------------------------------------------------------------
class ParallelMerge {
    private:
    const uint64Pair *leftPairs, *rightPairs;
    size_t pairsSize;
    uint64VecCc &indexPairs;

    public:

    void operator()(const tbb::blocked_range<size_t> &range) const {
        ParallelMerge::mergeJoin(this->leftPairs, this->rightPairs, pairsSize,
                                 range.begin(), range.end(), indexPairs);
    }

    void mergeJoin(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                   size_t size, size_t begin, size_t end, uint64VecCc &indexPairs) const;

    ParallelMerge(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                  size_t pairsSize, uint64VecCc &indexPairs) :
        leftPairs(leftPairs), rightPairs(rightPairs), pairsSize(pairsSize), indexPairs(indexPairs) { }
};
//---------------------------------------------------------------------------
uint64_t calcParallelSum(IteratorPair valIter);
//---------------------------------------------------------------------------
void getValuesIndexedParallel(const IteratorPair valIter, std::vector<uint64Pair> &pairs);
//---------------------------------------------------------------------------
