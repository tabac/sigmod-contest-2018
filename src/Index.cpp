#include "Index.hpp"
#include <algorithm>
#include <iostream>

// Class constructor
SortedIndex::SortedIndex(uint64_t  *values, uint64_t size) 
{		
	this->values.reserve(size);
	for(uint64_t i=0;i<size; i++) {
		this->values.push_back(new IdValuePair(i, values[i]));
	}
}

SortedIndex::~SortedIndex() {
	for(uint64_t i=0;i<this->values.size();i++) {
		delete this->values[i];
	}
}

// Builds the index
bool SortedIndex::build() 
{
	std::sort(this->values.begin(), this->values.end(), IdValuePair::compare);
	return true;
}

// Returns an iterator with the ids of the Tuples that satisfy FilterInfo
IteratorPair SortedIndex::getIdsIterator(FilterInfo* filterInfo)
{
	IteratorPair a;
	return a;
}

// Returns an iterator with the values of the Tuples that satisfy the FilterInfo
IteratorPair SortedIndex::getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo)
{
	IteratorPair a;
	return a;
}

uint64_t SortedIndex::findElement(uint64_t value) {
	uint64_t start = 0, finish = this->values.size()-1;
	uint64_t median;
	while(finish - start > 1) {
		median = (finish + start)/2;
		if (value > this->values[median]->value) {
			start = median;
		} else {
			finish = median;
		}
	}
	if(value < this->values[start]->value){
		return -1;
	}
	return (value < this->values[finish]->value? start : finish);
}
