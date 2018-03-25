#include <cassert>
#include <iostream>
#include <algorithm>
#include "Index.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
// Class constructor
SortedIndex::SortedIndex(const SelectInfo &selection, const IteratorPair valIter) : selection(selection)
{
    vector<IdValuePair> pairs;
    pairs.reserve(valIter.second - valIter.first);

    uint64_t i;
    vector<uint64_t>::const_iterator it;
    for (i = 0, it = valIter.first; it != valIter.second; ++i, ++it) {
        pairs.emplace_back(i, (*it));
    }

    sort(pairs.begin(), pairs.end());

    this->ids.reserve(pairs.size());
    this->values.reserve(pairs.size());

    vector<IdValuePair>::iterator jt;
    for (jt = pairs.begin(); jt != pairs.end(); ++jt) {
        this->ids.push_back(jt->id);
        this->values.push_back(jt->value);
    }
}

// Returns an iterator with the ids of the Tuples that satisfy FilterInfo
optional<IteratorPair> SortedIndex::getIdsIterator(const SelectInfo& selectInfo,
                                                   const FilterInfo* filterInfo)
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

// Returns an iterator with the values of the Tuples that satisfy the FilterInfo
optional<IteratorPair> SortedIndex::getValuesIterator(const SelectInfo& selectInfo,
                                                      const FilterInfo* filterInfo)
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

// returns the index of the specific element. If the element does not exist,
// the index of the smaller closer element is returned.
uint64_t SortedIndex::findElement(uint64_t value)
{
    uint64_t start = 0, finish = this->values.size() - 1;
    uint64_t median;
    while(finish - start > 1) {
        median = (finish + start)/2;
        if (value > this->values[median]) {
            start = median;
        } else { // equality
            finish = median;
        }
    }

    return (value < this->values[finish]? start : finish);
}
uint64Pair SortedIndex::estimateIndexes(const FilterInfo *filterInfo)
{
    uint64Pair range;

    if (filterInfo->comparison == FilterInfo::Comparison::Less){
        range.first = 0;

        range.second = this->findElement(filterInfo->constant);

        if(filterInfo->constant < this->values[range.second]){
            range.second = 0;
        }

        if(filterInfo->constant > this->values[range.second]){
            range.second += 1;
        }

    } else if (filterInfo->comparison == FilterInfo::Comparison::Greater){
        range.first = this->findElement(filterInfo->constant + 1);

        if(filterInfo->constant > this->values[range.first]){
            range.first = this->values.size();
        }

        range.second = this->values.size();


    } else if (filterInfo->comparison == FilterInfo::Comparison::Equal){
        range.first = this->findElement(filterInfo->constant);
        range.second = this->findElement(filterInfo->constant + 1);

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
