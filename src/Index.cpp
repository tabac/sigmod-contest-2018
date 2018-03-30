#include <chrono>
#include <thread>
#include <cassert>
#include <iostream>
#include <algorithm>
#include "Index.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void SortedIndex::buildIndex(void)
{
    this->setStatus(IndexStatus::building);

    vector<uint64Pair> pairs;
    pairs.reserve(valIter.second - valIter.first);

    uint64_t i;
    vector<uint64_t>::const_iterator it;
    for (i = 0, it = valIter.first; it != valIter.second; ++i, ++it) {
        pairs.emplace_back(i, (*it));
    }

    sort(pairs.begin(), pairs.end(),
         [&](const uint64Pair &a, const uint64Pair &b) { return a.second < b.second; });

    this->ids.reserve(pairs.size());
    this->values.reserve(pairs.size());

    vector<uint64Pair>::iterator jt;
    for (jt = pairs.begin(); jt != pairs.end(); ++jt) {
        this->ids.emplace_back(jt->first);
        this->values.emplace_back(jt->second);
    }

    this->setStatus(IndexStatus::ready);

    assert(this->isStatusReady());
}
//---------------------------------------------------------------------------
optional<IteratorPair> SortedIndex::getIdsIterator(const SelectInfo& selectInfo,
                                                   const FilterInfo* filterInfo)
// Returns an iterator with the ids of the Tuples that satisfy FilterInfo
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

        assert(range.first <= this->ids.size() && range.second <= this->ids.size());

        return optional<IteratorPair>{{
            this->ids.begin() + range.first,
            this->ids.begin() + range.second
        }};
    } else {
        return optional<IteratorPair>{{this->ids.begin(), this->ids.end()}};
    }
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
void SortedIndex::getValuesIndexedSorted(vector<uint64Pair> &pairs)
{
    {
        assert(pairs.empty());
    }

    vector<uint64_t>::iterator it, jt;
    for (it = this->ids.begin(), jt = this->values.begin(); it != this->ids.end(); ++it, ++jt) {
        pairs.emplace_back((*it), (*jt));
    }
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
