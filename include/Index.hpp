#pragma once
#include "Parser.hpp"
#include "Relation.hpp"

// ---------------------------------------------------------------------------
// Index class is the base class for all Index implementations.
class AbstractIndex {
	public:
	// Defines whether the index is built online (i.e., for each query) or
	// offline
	bool online;
	// Defines the relation/column for which the index is built
	SelectInfo selectInfo; 

	AbstractIndex(){
		this->online = true; this->selectInfo = new SelectInfo
	};
	// Method used to construct the index, returning the index builiding 
	// status. If used when onlineConstruction == true returns false.
	virtual bool build() = 0;
};
// ---------------------------------------------------------------------------
// SortedIndex represents a simple index that sorts a column of a Relation
// and responds to queries based on DataReaderMixin
class SortedIndex : public AbstractIndex {
	public:
	int *data;
	bool build();
	SortedIndex(bool online, SelectInfo& info);
	IteratorPair getIdsIterator(FilterInfo* filterInfo);
	IteratorPair getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo);
};

