#pragma once
#include "Mixins.hpp"
#include "Parser.hpp"

class IdValuePair{
	public:
	uint64_t id, value;
	IdValuePair(){};
	IdValuePair(uint64_t id, uint64_t value): id(id), value(value){};

	bool operator <(const IdValuePair& o)
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
	//	bool online;

//	std::vector <IdValuePair *> values;
	uint64_t *ids, *values, size;

};
// ---------------------------------------------------------------------------
// SortedIndex represents a simple index that sorts a column of a Relation
// and responds to queries based on DataReaderMixin
class SortedIndex : public AbstractIndex {
//class SortedIndex : class AbstractIndex {
	public:
	// Constructor
	SortedIndex(uint64_t *values, uint64_t size);
	~SortedIndex();

	// Returns the ids that match the filterInfo
	IteratorPair getIdsIterator(FilterInfo* filterInfo);
	//Returns the values that match filterInfo
    std::optional<IteratorPair> getValuesIterator(SelectInfo& selectInfo,
                                                  FilterInfo* filterInfo);

	private:
	// findElement traverses the index and returns the position of the
	// specified value. If the specified value does not exist, it returns the
	// index of the directly smallest value.
	uint64_t findElement(uint64_t value);
	void estimateIndexes(uint64_t *, uint64_t *, FilterInfo *);
};
