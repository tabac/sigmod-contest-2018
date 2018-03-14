#include "Index.hpp"

// Class constructor
SortedIndex::SortedIndex(bool online, uint64_t  *values, uint64_t size) 
{		
	this->online = online;

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
	if(this->online) { 	//building for each call separately
		return false;
	}
	// start building my shit
	return this->buildOffline();
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

bool SortedIndex::buildOffline() {
	uint64_t *indexes, *values;
	return true;
}

bool SortedIndex::buildIncrementally() {
	return true;
}
