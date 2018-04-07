#include <chrono>
#include <thread>
#include <cassert>
#include <iostream>
#include <algorithm>
#include <tbb/parallel_sort.h>
#include "Index.hpp"
#include "Parallel.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void SortedIndex::buildIndex(void)
{
    this->setStatus(IndexStatus::building);

    const size_t valuesSize = this->valIter.second- this->valIter.first;

    if (valuesSize != 0) {
        getValuesIndexedParallel(this->valIter, this->idValuePairs);

        tbb::parallel_sort(this->idValuePairs.begin(), this->idValuePairs.end(),
             [&](const uint64Pair &a, const uint64Pair &b) { return a.second < b.second; });

        this->values.reserve(this->idValuePairs.size());

        // TODO: This in parallel too...
        vector<uint64Pair>::iterator jt;
        for (jt = this->idValuePairs.begin(); jt != this->idValuePairs.end(); ++jt) {
            this->values.emplace_back(jt->second);
        }
    }

    this->setStatus(IndexStatus::ready);

    assert(this->isStatusReady());
}
//---------------------------------------------------------------------------
optional<IteratorPair> SortedIndex::getIdsIterator(const SelectInfo& selectInfo,
                                                   const FilterInfo* filterInfo)
{
    {
        assert(false);

        assert(selectInfo.relId == this->selection.relId &&
               selectInfo.colId == this->selection.colId);

        assert(filterInfo == NULL ||
               (filterInfo->filterColumn.relId == this->selection.relId &&
                filterInfo->filterColumn.colId == this->selection.colId));
    }

    return nullopt;
}
//---------------------------------------------------------------------------
optional<IteratorPair> SortedIndex::getValuesIterator(const SelectInfo& selectInfo,
                                                      const FilterInfo* filterInfo)
// Returns an iterator with the values of the Tuples that satisfy the FilterInfo
{
    {
        assert(selectInfo.relId == this->selection.relId &&
               selectInfo.colId == this->selection.colId);

        assert(filterInfo == NULL ||
               (filterInfo->filterColumn.relId == this->selection.relId &&
                filterInfo->filterColumn.colId == this->selection.colId));
    }

    if (filterInfo != NULL) {
        uint64Pair range = this->estimateIndexes(filterInfo);

        assert(range.first <= this->values.size() && range.second <= this->values.size());

        return optional<IteratorPair>{{
            this->values.begin() + range.first,
            this->values.begin() + range.second
        }};
    } else {
        return optional<IteratorPair>{{this->values.begin(), this->values.end()}};
    }
}
//---------------------------------------------------------------------------
optional<IteratorDoublePair> SortedIndex::getIdsValuesIterator(const SelectInfo& selectInfo,
                                                               const FilterInfo* filterInfo)
{
    {
        assert(selectInfo.relId == this->selection.relId &&
               selectInfo.colId == this->selection.colId);

        assert(filterInfo == NULL ||
               (filterInfo->filterColumn.relId == this->selection.relId &&
                filterInfo->filterColumn.colId == this->selection.colId));
    }

    if (filterInfo != NULL) {
        uint64Pair range = this->estimateIndexes(filterInfo);

        assert(range.first <= this->idValuePairs.size() &&
               range.second <= this->idValuePairs.size());

        return optional<IteratorDoublePair>{{
            this->idValuePairs.begin() + range.first,
            this->idValuePairs.begin() + range.second
        }};
    } else {
        return optional<IteratorDoublePair>{{
            this->idValuePairs.begin(),
            this->idValuePairs.end()
        }};
    }
}
//---------------------------------------------------------------------------
vector<uint64Pair> *SortedIndex::getValuesIndexedSorted(void)
{
    return &this->idValuePairs;
}
//---------------------------------------------------------------------------
uint64_t SortedIndex::findElement(uint64_t value)
{
    uint64_t l = 0, r = this->values.size();
    while(r > l) {
        uint64_t m = (l + r) / 2;

        if (value > this->values[m]) {
            l = m + 1;
        } else {
            r = m;
        }
    }

    return l;
}
//---------------------------------------------------------------------------
uint64Pair SortedIndex::estimateIndexes(const FilterInfo *filterInfo)
{
    uint64Pair range;

    if (filterInfo->comparison == FilterInfo::Comparison::Less){
        range = {0, this->findElement(filterInfo->constant)};
    } else if (filterInfo->comparison == FilterInfo::Comparison::Greater){
        range = {this->findElement(filterInfo->constant + 1), this->values.size()};
    } else if (filterInfo->comparison == FilterInfo::Comparison::Equal){
        range = {
            this->findElement(filterInfo->constant),
            this->findElement(filterInfo->constant + 1)
        };

        if(this->values[range.second] == filterInfo->constant) {
            range.second +=1;
        }
    }

    return range;
}
// ---------------------------------------------------------------------------
