#include <cstdint>
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
void ParallelMerge::mergeJoin(const uint64Pair *leftPairs, const uint64Pair *rightPairs,
                              size_t size, size_t begin, size_t end, uint64VecCc &indexPairs) const
{
    size_t l = begin, r = 0;
    vector<uint64Pair> indexPairsLoc;

    if (begin == end || size == 0) {
        return;
    }

    indexPairsLoc.reserve(end - begin);

    __builtin_prefetch(&leftPairs[l], 0, 2);
    __builtin_prefetch(&rightPairs[r], 0, 2);
    uint64_t left = leftPairs[l].second;
    uint64_t right = rightPairs[r].second;
    while (true) {
        while (left < right && l < end) {
            left = leftPairs[++l].second;
        }
        if (l == end) {
            break;
        }

        while (right < left && r < size) {
            right = rightPairs[++r].second;
        }
        if (r == size) {
            break;
        }

        if (left == right) {
            uint64_t leftIndex = leftPairs[l].first;
            for (size_t t = r; t < size && left == rightPairs[t].second; ++t) {
                indexPairsLoc.emplace_back(leftIndex, rightPairs[t].first);
            }

            ++l;

            if (l == end) {
                break;
            } else {
                left = leftPairs[l].second;
            }
        }
    }

    indexPairs.grow_by(indexPairsLoc.begin(), indexPairsLoc.end());
}
//---------------------------------------------------------------------------
