#pragma once
#include "Parser.hpp"
#include "Mixins.hpp"
// ---------------------------------------------------------------------------
enum IndexStatus { building, ready };
// ---------------------------------------------------------------------------
class AbstractIndex {
    /// Index class is the base class for all Index implementations.
    public:
    /// Status of the index, whether `ready` or `building`.
    IndexStatus status;
    /// The indexed column.
    const SelectInfo &selection;
    /// Vectors that hold sorted `ids`, `values` for the given `selection`.
    std::vector<uint64_t> ids, values;

    /// Status setter.
    void setStatus(IndexStatus status) { this->status = status; };

    /// Status getters.
    bool isStatusBuilding() { return status == IndexStatus::building; };
    bool isStatusReady() { return status == IndexStatus::ready; };

    AbstractIndex(const SelectInfo &selection) : selection(selection) { }
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

    // Returns the ids that match the filterInfo.
    std::optional<IteratorPair> getIdsIterator(const SelectInfo& selectInfo,
                                               const FilterInfo* filterInfo);
    //Returns the values that match filterInfo.
    std::optional<IteratorPair> getValuesIterator(const SelectInfo& selectInfo,
                                                  const FilterInfo* filterInfo);

    void getValuesIndexedSorted(std::vector<uint64Pair> &pairs);

    /// Constructor.
    SortedIndex(const SelectInfo &selection, const IteratorPair valIter);
};
// ---------------------------------------------------------------------------
