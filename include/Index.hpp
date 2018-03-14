#pragma once
#include "Parser.hpp"
#include "Relation.hpp"
#include "Mixins.hpp"

class IdValuePair{
	public:
	uint64_t id, value;
	IdValuePair(){};
	IdValuePair(uint64_t id, uint64_t value): id(id), value(value){};

	bool operator<(const IdValuePair &o) 
	{
			return this->value < o.value;
	}
};


// ---------------------------------------------------------------------------
// Index class is the base class for all Index implementations.
class AbstractIndex: public DataReaderMixin {
	public:
	// Defines whether the index is built online (i.e., for each query) or
	// offline
	bool online;

	std::vector <IdValuePair *> values;

	// Method used to construct the index, returning the index builiding 
	// status. If used when onlineConstruction == true returns false.
	virtual bool build() = 0;
};
// ---------------------------------------------------------------------------
// SortedIndex represents a simple index that sorts a column of a Relation
// and responds to queries based on DataReaderMixin
class SortedIndex : public AbstractIndex {
//class SortedIndex : class AbstractIndex {
	public:
	// Constructor
	SortedIndex(bool online, uint64_t *values, uint64_t size);
	~SortedIndex();

	// Builds the index
	bool build();
	// Returns the ids that match the filterInfo
	IteratorPair getIdsIterator(FilterInfo* filterInfo);
	//Returns the values that match filterInfo
	IteratorPair getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo);

	private:
	bool buildOffline();
	bool buildIncrementally();
};

