#include <cassert>
#include <iostream>
#include <algorithm>
#include "Index.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
SortedIndex::SortedIndex(const SelectInfo &selection, const IteratorPair valIter) : AbstractIndex(selection)
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
}
//---------------------------------------------------------------------------
optional<IteratorPair> SortedIndex::getIdsIterator(const SelectInfo& selectInfo,
                                                   const FilterInfo* filterInfo)
// Returns an iterator with the ids of the Tuples that satisfy FilterInfo
{
    {
        assert(selectInfo == this->selection);

        assert(filterInfo != NULL && filterInfo->filterColumn == this->selection);
    }

    uint64Pair range = this->estimateIndexes(filterInfo);

    assert(range.first <= this->ids.size() && range.second <= this->ids.size());

    return optional<IteratorPair>{{
        this->ids.begin() + range.first,
        this->ids.begin() + range.second
    }};
}
//---------------------------------------------------------------------------
optional<IteratorPair> SortedIndex::getValuesIterator(const SelectInfo& selectInfo,
                                                      const FilterInfo* filterInfo)
// Returns an iterator with the values of the Tuples that satisfy the FilterInfo
{
    {
        assert(selectInfo == this->selection);

        assert(filterInfo != NULL && filterInfo->filterColumn == this->selection);
    }

    uint64Pair range = this->estimateIndexes(filterInfo);

    assert(range.first <= this->values.size() && range.second <= this->values.size());

    return optional<IteratorPair>{{
        this->values.begin() + range.first,
        this->values.begin() + range.second
    }};
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
optional<IteratorPair> AdaptiveIndex::getIdsIterator(const SelectInfo& selectInfo,
                                                     const FilterInfo* filterInfo)
{
    return nullopt;
}
// ---------------------------------------------------------------------------
optional<IteratorPair> AdaptiveIndex::getValuesIterator(const SelectInfo& selectInfo,
                                                        const FilterInfo* filterInfo)
{
    return nullopt;
}
// ---------------------------------------------------------------------------
AdaptiveIndex::AdaptiveIndex(const SelectInfo &selection, const IteratorPair values) : selection(selection)
{
    // this->selection = selection;
}
AdaptiveIndex::AdaptiveIndex(const SelectInfo &selection, const FilterInfo &filter,
                             const IteratorPair values) : selection(selection)
{
    // this->selection = selection;
}
// ---------------------------------------------------------------------------
