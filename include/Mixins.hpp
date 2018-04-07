#pragma once
#include <mutex>
#include <vector>
#include <cstdint>
#include <optional>
#include <condition_variable>
#include <experimental/optional>
#include <tbb/tbb.h>
//---------------------------------------------------------------------------
struct FilterInfo;
struct SelectInfo;
//---------------------------------------------------------------------------
#define DEBUG(x) do { std::cerr << x ; } while (0)
#define DEBUGLN(x) do { std::cerr << x << std::endl; } while (0)
//---------------------------------------------------------------------------
#define optional std::experimental::optional
#define nullopt  std::experimental::nullopt
//---------------------------------------------------------------------------
using RelationId = unsigned;
//---------------------------------------------------------------------------
using uint64Pair = std::pair<uint64_t, uint64_t>;
//---------------------------------------------------------------------------
using unsignedPair = std::pair<unsigned, unsigned>;
//---------------------------------------------------------------------------
using SyncPair = std::pair<std::mutex, std::condition_variable>;
//---------------------------------------------------------------------------
using uint64VecCc = tbb::concurrent_vector<uint64Pair>;
//---------------------------------------------------------------------------
using uint64VecMapCc = tbb::concurrent_unordered_map<uint64_t, tbb::concurrent_vector<uint64_t>>;
//---------------------------------------------------------------------------
using IteratorPair = std::pair<std::vector<uint64_t>::const_iterator,
                               std::vector<uint64_t>::const_iterator>;
//---------------------------------------------------------------------------
using IteratorDoublePair = std::pair<std::vector<uint64Pair>::const_iterator,
                                     std::vector<uint64Pair>::const_iterator>;
//---------------------------------------------------------------------------
static const bool INDEXES_ON = true;
static const bool INDEXES_CREATE_ON_MERGE = true;
static const bool CHECK_SORTED_SELECTIONS = true;
static const size_t PAIRS_GRAIN_SIZE = 256;
static const size_t SINGLES_GRAIN_SIZE = 512;
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
    virtual optional<IteratorPair> getIdsIterator(const SelectInfo& selectInfo,
                                                  const FilterInfo* filterInfo) = 0;

    /// Should be implemented by any class intended as a data storer
    /// and return the values of the column specified by `selectInfo
    /// that satisfy the `filterInfo` condition.
    ///
    /// In our case `DataNode`, `Relation` and `Index`.
    /// In the first two cases `filterInfo` is ignored, in the
    /// later the `index` should use `filterInfo` to narrow down the
    /// return range of values.
    virtual optional<IteratorPair> getValuesIterator(const SelectInfo& selectInfo,
                                                     const FilterInfo* filterInfo) = 0;

    virtual ~DataReaderMixin() { }
};
//---------------------------------------------------------------------------
template <class T>
inline void hash_combine(std::size_t& seed, const T& v)
{
    std::hash<T> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

namespace std
{
    template<typename S, typename T>
    struct hash<pair<S, T>>
    {
        inline size_t operator()(const pair<S, T>& v) const
        {
            size_t seed = 0;
            ::hash_combine(seed, v.first);
            ::hash_combine(seed, v.second);

            return seed;
        }
    };

    template<>
    struct hash<std::tuple<unsigned , unsigned , unsigned >>
    {
        inline size_t operator()(const std::tuple<unsigned , unsigned , unsigned >& v) const
        {
            size_t seed = 0;
            ::hash_combine(seed, std::get<0>(v));
            ::hash_combine(seed, std::get<1>(v));
            ::hash_combine(seed, std::get<2>(v));

            return seed;
        }
    };
}
//---------------------------------------------------------------------------
