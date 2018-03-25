#pragma once
#include "Parser.hpp"
#include "Mixins.hpp"

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
class AbstractIndex {
    public:
    std::vector<uint64_t> ids, values;
};
// ---------------------------------------------------------------------------
// SortedIndex represents a simple index that sorts a column of a Relation
// and responds to queries based on DataReaderMixin
class SortedIndex : public AbstractIndex, public DataReaderMixin {
    private:
    // findElement traverses the index and returns the position of the
    // specified value. If the specified value does not exist, it returns the
    // index of the directly smallest value.
    uint64_t findElement(uint64_t value);

    uint64Pair estimateIndexes(const FilterInfo *);

    public:
    /// The indexed column.
    const SelectInfo &selection;

    // Returns the ids that match the filterInfo.
    std::optional<IteratorPair> getIdsIterator(const SelectInfo& selectInfo,
                                               const FilterInfo* filterInfo);
    //Returns the values that match filterInfo.
    std::optional<IteratorPair> getValuesIterator(const SelectInfo& selectInfo,
                                                  const FilterInfo* filterInfo);

    /// Constructor.
    SortedIndex(const SelectInfo &selection, const IteratorPair valIter);
};
// ---------------------------------------------------------------------------
class AdaptiveIndex : public DataReaderMixin {
    public:

    const SelectInfo &selection;

    std::vector<uint64_t> ids[2], values[2];

    std::optional<IteratorPair> getIdsIterator(const SelectInfo& selectInfo,
                                               const FilterInfo* filterInfo);

    std::optional<IteratorPair> getValuesIterator(const SelectInfo& selectInfo,
                                                  const FilterInfo* filterInfo);

    AdaptiveIndex(const SelectInfo &selection, const IteratorPair valIter);

    AdaptiveIndex(const SelectInfo &selection, const FilterInfo &filterInfo,
                  const IteratorPair valIter);

   ~AdaptiveIndex();
};
// ---------------------------------------------------------------------------
