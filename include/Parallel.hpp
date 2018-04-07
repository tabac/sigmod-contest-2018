#pragma once
#include <iostream>
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
        const T &indicesLoc = this->indices;
        uint64_t *outValuesLoc = this->outValues;

        size_t end = range.end();

        __builtin_prefetch(&indicesLoc[range.begin()], 0, 0);
        for(size_t i = range.begin(); i != end; ++i) {
            outValuesLoc[i] = inValuesLoc[std::get<I>(indicesLoc[i])];
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
};
//---------------------------------------------------------------------------
class ParallelMapBuild {
    private:
    uint64VecMapCc  &map;
    const uint64Pair *pairs;

    public:
    void operator()(const tbb::blocked_range<size_t>& range) const {
        const uint64Pair *pairsLoc = this->pairs;
        uint64VecMapCc &mapLoc = this->map;

        size_t end = range.end();

        __builtin_prefetch(&pairsLoc[range.begin()], 0, 0);
        for(size_t i = range.begin(); i != end; ++i) {
            mapLoc[pairsLoc[i].second].push_back(pairsLoc[i].first);
        }
    }

    ParallelMapBuild(const uint64Pair *pairs, uint64VecMapCc &map) : map(map), pairs(pairs) { }
};
//---------------------------------------------------------------------------
template <bool B>
class ParallelMapProbe {
    private:
    const uint64Pair *pairs;
    const uint64VecMapCc  &map;
    uint64VecCc &indexPairs;

    public:

    void operator()(const tbb::blocked_range<size_t> &range) const {
        const uint64Pair *pairsLoc = this->pairs;
        const uint64VecMapCc &mapLoc = this->map;

        std::vector<uint64Pair> indexPairsLoc;
        indexPairsLoc.reserve(range.end() - range.begin());

        size_t end = range.end();

        if (!B) {
            __builtin_prefetch(&pairsLoc[range.begin()], 0, 0);
            for(size_t i = range.begin(); i != end; ++i) {

                const uint64VecMapCc::const_iterator kv = mapLoc.find(pairsLoc[i].second);

                if (kv != mapLoc.end()) {
                    const tbb::concurrent_vector<uint64_t> &bucket = kv->second;

                    tbb::concurrent_vector<uint64_t>::const_iterator jt;
                    for (jt = bucket.begin(); jt != bucket.end(); ++jt) {
                        indexPairsLoc.emplace_back((*jt), pairsLoc[i].first);
                    }
                }
            }
        } else {
            __builtin_prefetch(&pairsLoc[range.begin()], 0, 0);
            for(size_t i = range.begin(); i != end; ++i) {

                const uint64VecMapCc::const_iterator kv = mapLoc.find(pairsLoc[i].second);

                if (kv != mapLoc.end()) {
                    const tbb::concurrent_vector<uint64_t> &bucket = kv->second;

                    tbb::concurrent_vector<uint64_t>::const_iterator jt;
                    for (jt = bucket.begin(); jt != bucket.end(); ++jt) {
                        indexPairsLoc.emplace_back(pairsLoc[i].first, (*jt));
                    }
                }
            }
        }

        this->indexPairs.grow_by(indexPairsLoc.begin(), indexPairsLoc.end());
    }


    ParallelMapProbe(const uint64Pair *pairs, const uint64VecMapCc &map, uint64VecCc &indexPairs) :
        pairs(pairs), map(map), indexPairs(indexPairs) { }
};
//---------------------------------------------------------------------------
template <bool B>
class ParallelMerge {
    private:
    const uint64Pair *leftPairs, *rightPairs;
    size_t pairsSize;
    uint64VecCc &indexPairs;

    public:

    void operator()(const tbb::blocked_range<size_t> &range) const {
        const uint64Pair *leftPairsLoc = this->leftPairs;
        const uint64Pair *rightPairsLoc = this->rightPairs;

        const size_t size = this->pairsSize;
        const size_t begin = range.begin();
        const size_t end = range.end();

        size_t l = begin, r = 0;
        std::vector<uint64Pair> indexPairsLoc;

        if (begin == end || size == 0) {
            return;
        }

        indexPairsLoc.reserve(end - begin);

        __builtin_prefetch(&leftPairsLoc[l], 0, 0);
        __builtin_prefetch(&rightPairsLoc[r], 0, 0);

        uint64_t left = leftPairsLoc[l].second;
        uint64_t right = rightPairsLoc[r].second;
        if (!B) {
            for ( ;; ) {
                while (l < end && leftPairsLoc[l].second < right) {
                    ++l;
                }
                if (l == end) {
                    break;
                }
                left = leftPairsLoc[l].second;

                while (r < size && rightPairsLoc[r].second < left) {
                    ++r;
                }
                if (r == size) {
                    break;
                }
                right = rightPairsLoc[r].second;

                if (left == right) {
                    uint64_t leftIndex = leftPairsLoc[l].first;
                    for (size_t t = r; t < size && left == rightPairsLoc[t].second; ++t) {
                        indexPairsLoc.emplace_back(leftIndex, rightPairsLoc[t].first);
                    }

                    ++l;
                }
            }
        } else {
            for ( ;; ) {
                while (l < end && leftPairsLoc[l].second < right) {
                    ++l;
                }
                if (l == end) {
                    break;
                }
                left = leftPairsLoc[l].second;

                while (r < size && rightPairsLoc[r].second < left) {
                    ++r;
                }
                if (r == size) {
                    break;
                }
                right = rightPairsLoc[r].second;

                if (left == right) {
                    uint64_t leftIndex = leftPairsLoc[l].first;
                    for (size_t t = r; t < size && left == rightPairsLoc[t].second; ++t) {
                        indexPairsLoc.emplace_back(rightPairsLoc[t].first, leftIndex);
                    }

                    ++l;
                }
            }
        }

        if (!indexPairsLoc.empty()) {
            this->indexPairs.grow_by(indexPairsLoc.begin(), indexPairsLoc.end());
        }
    }

    ParallelMerge(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                  size_t pairsSize, uint64VecCc &indexPairs) :
        leftPairs(leftPairs), rightPairs(rightPairs), pairsSize(pairsSize), indexPairs(indexPairs) { }
};
//---------------------------------------------------------------------------
uint64_t calcParallelSum(IteratorPair valIter);
//---------------------------------------------------------------------------
void getValuesIndexedParallel(const IteratorPair valIter, std::vector<uint64Pair> &pairs);
//---------------------------------------------------------------------------
