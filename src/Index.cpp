#include "Index.hpp"

// Class constructor
SortedIndex::SortedIndex(bool online, SelectInfo& info) 
{
		
	this->online = online;
	this->selectInfo = info;
}

// Builds the index
bool SortedIndex::build() 
{
	return false;
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
