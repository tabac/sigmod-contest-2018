#pragma once
#include <vector>
#include <cstdint>
//---------------------------------------------------------------------------
struct FilterInfo;
struct SelectInfo;
//---------------------------------------------------------------------------
using RelationId = unsigned;
//---------------------------------------------------------------------------
using IteratorPair = std::pair<std::vector<uint64_t>::iterator, std::vector<uint64_t>::iterator>;
//---------------------------------------------------------------------------
class DataReaderMixin {
    // TODO: Would be nice not to pass `filterInfo` and then ignore it...

    public:
    /// Should be implemented by any class intended as a data storer
    /// and return the rowIds that satisfy the `filterInfo` condition.
    ///
    /// In our case `DataNode`, `Relation` and `Index`.
    /// In the first two cases `filterInfo` is ignored, in the
    /// later the `index` should use `filterInfo` to narrow down the
    /// return range of ids.
    virtual IteratorPair getIdsIterator(FilterInfo* filterInfo) = 0;

    /// Should be implemented by any class intended as a data storer
    /// and return the values of the column specified by `selectInfo
    /// that satisfy the `filterInfo` condition.
    ///
    /// In our case `DataNode`, `Relation` and `Index`.
    /// In the first two cases `filterInfo` is ignored, in the
    /// later the `index` should use `filterInfo` to narrow down the
    /// return range of values.
    virtual IteratorPair getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo) = 0;
};
//---------------------------------------------------------------------------
