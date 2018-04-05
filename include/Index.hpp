#pragma once
#include "Parser.hpp"
#include "Mixins.hpp"
// ---------------------------------------------------------------------------
enum IndexStatus { uninitialized, building, ready };
// ---------------------------------------------------------------------------
class AbstractIndex {
    /// Index class is the base class for all Index implementations.
    public:
    /// Status of the index, whether `ready` or `building`.
    IndexStatus status;
    /// The indexed column.
    const SelectInfo selection;
    /// Vectors that hold sorted `ids`, `values` for the given `selection`.
    std::vector<uint64_t> values;
    /// Vector that holds `{id, value}` pairs, sorted by value.
    std::vector<uint64Pair> idValuePairs;

    /// Status setter.
    void setStatus(IndexStatus status) { this->status = status; };

    /// Status getters.
    bool isStatusBuilding() { return status == IndexStatus::building; };
    bool isStatusReady() { return status == IndexStatus::ready; };
    bool isStatusUninitialized() { return status == IndexStatus::uninitialized; };

    AbstractIndex(const SelectInfo &selection) :
        status(IndexStatus::uninitialized), selection(selection) { }
};
// ---------------------------------------------------------------------------
class SortedIndex : public AbstractIndex, public DataReaderMixin {
    /// SortedIndex represents a simple index that sorts a column of a Relation
    /// and responds to queries based on DataReaderMixin

    private:
    /// findElement traverses the index and returns the position of the
    /// specified value. If the specified value does not exist, it returns the
    /// index of the directly smallest value.
    uint64_t findElement(uint64_t value);

    uint64Pair estimateIndexes(const FilterInfo *);

    public:

    const IteratorPair valIter;

    // Returns the ids that match the filterInfo.
    optional<IteratorPair> getIdsIterator(const SelectInfo& selectInfo,
                                          const FilterInfo* filterInfo);
    //Returns the values that match filterInfo.
    optional<IteratorPair> getValuesIterator(const SelectInfo& selectInfo,
                                             const FilterInfo* filterInfo);

    optional<IteratorDoublePair> getIdsValuesIterator(const SelectInfo& selectInfo,
                                                      const FilterInfo* filterInfo);

    std::vector<uint64Pair> *getValuesIndexedSorted(void);

    void buildIndex(void);

    /// Constructor.
    SortedIndex(const SelectInfo &selection, const IteratorPair valIter) :
        AbstractIndex(selection), valIter(valIter) { }
};
// ---------------------------------------------------------------------------
