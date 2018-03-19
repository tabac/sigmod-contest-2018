#include <algorithm>
#include "Index.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
// Class constructor
SortedIndex::SortedIndex(uint64_t  *values, uint64_t size)
{
	vector<IdValuePair> temp_values;
	temp_values.reserve(size);
	for(uint64_t i=0;i<size; i++) {
		temp_values.push_back(IdValuePair(i, values[i]));
	}
	sort(temp_values.begin(), temp_values.end());

	this->ids = new uint64_t[temp_values.size()];
	this->values = new uint64_t[temp_values.size()];

	for(uint64_t i=0;i<temp_values.size();i++) {
		this->ids[i] = temp_values[i].id;
		this->values[i] = temp_values[i].value;
	}
	this->size = size;
}

// Destructor
SortedIndex::~SortedIndex() {
	delete[] this->ids;
	delete[] this->values;
}

// Returns an iterator with the ids of the Tuples that satisfy FilterInfo
IteratorPair SortedIndex::getIdsIterator(FilterInfo* filterInfo)
{
	uint64_t idxFrom, idxTo;
	this->estimateIndexes(&idxFrom, &idxTo, filterInfo);
	vector<uint64_t>::iterator begin(&this->ids[idxFrom]);
	vector<uint64_t>::iterator end(&this->ids[idxTo]);
	return {begin, end};
}

// Returns an iterator with the values of the Tuples that satisfy the FilterInfo
optional<IteratorPair> SortedIndex::getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo)
{
	uint64_t idxFrom, idxTo;
	this->estimateIndexes(&idxFrom, &idxTo, filterInfo);
	vector<uint64_t>::iterator begin(&this->values[idxFrom]);
	vector<uint64_t>::iterator end(&this->values[idxTo]);

    return optional<IteratorPair>{{begin, end}};
}

// returns the index of the specific element. If the element does not exist,
// the index of the smaller closer element is returned.
uint64_t SortedIndex::findElement(uint64_t value)
{
	uint64_t start = 0, finish = this->size-1;
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

void SortedIndex::estimateIndexes(uint64_t *idxFrom, uint64_t *idxTo, FilterInfo *filterInfo) {
	if (filterInfo->comparison == FilterInfo::Comparison::Less){
		*idxFrom = 0;
		*idxTo = this->findElement(filterInfo->constant);
		if(filterInfo->constant < this->values[*idxTo]){
			*idxTo=0;
		}
		if(filterInfo->constant > this->values[*idxTo]){
			*idxTo += 1;
		}
	} else if (filterInfo->comparison == FilterInfo::Comparison::Greater){
		*idxFrom = this->findElement(filterInfo->constant+1);
		if(filterInfo->constant >= this->values[*idxFrom]){
			*idxFrom = this->size;
		}
		*idxTo = this->size;

	} else if (filterInfo->comparison == FilterInfo::Comparison::Equal){
		*idxFrom = this->findElement(filterInfo->constant);
		*idxTo = this->findElement(filterInfo->constant+1);
		if(this->values[*idxTo] == filterInfo->constant) {
				*idxTo +=1;
		}
	}

}
