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

        for(size_t i = range.begin(); i != end; ++i) {
            outValuesLoc[i] = inValuesLoc[std::get<I>(indices[i])];
        }
    }

    ParallelPush(const uint64_t *inValues, const T &indices, uint64_t *outValues) :
        inValues(inValues), indices(indices), outValues(outValues) { }
};
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
class ParallelMergeR {
    public:
    const uint64Pair *leftPairs, *rightPairs;
    size_t pairsSize;
    std::vector<uint64Pair> indexPairs;

    void operator()(const tbb::blocked_range<size_t>& range) {
        mergeJoin(range.begin(), range.end());
    }

    void mergeJoin(size_t begin, size_t end);

    void join(const ParallelMergeR &o) {
        this->indexPairs.reserve(this->indexPairs.size() + o.indexPairs.size());

        this->indexPairs.insert(this->indexPairs.end(),
                                o.indexPairs.begin(), o.indexPairs.end());
    }

    std::vector<uint64Pair> getIndexPairs(void) { return this->indexPairs; }

    ParallelMergeR(ParallelMergeR& x, tbb::split) :
        leftPairs(x.leftPairs), rightPairs(x.rightPairs), pairsSize(x.pairsSize) { }

    ParallelMergeR(const uint64Pair *leftPairs, const uint64Pair *rightPairs, size_t pairsSize) :
        leftPairs(leftPairs), rightPairs(rightPairs), pairsSize(pairsSize) { }
};
//---------------------------------------------------------------------------
template <typename T>
class ParallelMergeV {
    private:
    const uint64Pair *leftPairs, *rightPairs;
    size_t pairsSize;
    T &indexPairs;

    public:
    void operator()(const tbb::blocked_range<size_t> &range) const {
        ParallelMergeV::mergeJoin(this->leftPairs, this->rightPairs, pairsSize,
                                 range.begin(), range.end(), indexPairs);
    }

    void mergeJoin(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                   size_t size, size_t begin, size_t end, T &indexPairs) const
    {
        size_t l = begin, r = 0;
        std::vector<uint64Pair> *indexPairsLoc = new std::vector<uint64Pair>();

        indexPairsLoc->reserve(end - begin);

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
                    indexPairsLoc->emplace_back(leftIndex, rightPairs[t].first);
                }

                ++l;
            }
        }

        indexPairs.push_back(indexPairsLoc);
    }

    ParallelMergeV(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                  size_t pairsSize, T &indexPairs) :
        leftPairs(leftPairs), rightPairs(rightPairs), pairsSize(pairsSize), indexPairs(indexPairs) { }
};
//---------------------------------------------------------------------------
uint64_t calcParallelSum(IteratorPair valIter);
//---------------------------------------------------------------------------
