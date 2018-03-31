#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "Mixins.hpp"
//---------------------------------------------------------------------------
struct SelectInfo {
    /// Relation id
    RelationId relId;
    /// Binding for the relation
    unsigned binding;
    /// Column id
    unsigned colId;
    /// The constructor
    SelectInfo(RelationId relId,unsigned b,unsigned colId) : relId(relId), binding(b), colId(colId){};
    /// Equality operator
    bool operator==(const SelectInfo& o) const;
    /// Dump text format
    std::string dumpText();
    /// Dump graph label in text format
    std::string dumpLabel() const;
    /// Dump SQL
    std::string dumpSQL(bool addSUM=false);
    /// Equality operator, ignoring binding information.
    bool equalsRelationColumn(const SelectInfo &o) const;

    /// The delimiter used in our text format
    static const char delimiter=' ';
    /// The delimiter used in SQL
    constexpr static const char delimiterSQL[]=", ";
};
//---------------------------------------------------------------------------
struct FilterInfo {
    enum Comparison : char { Less='<', Greater='>', Equal='=' };
    /// Filter Column
    SelectInfo filterColumn;
    /// Constant
    uint64_t constant;
    /// Comparison type
    Comparison comparison;
    /// Dump SQL
    std::string dumpSQL();

    /// The constructor
    FilterInfo(SelectInfo filterColumn,uint64_t constant,Comparison comparison) : filterColumn(filterColumn), constant(constant), comparison(comparison) {};
    /// Dump text format
    std::string dumpText();
    /// Dump label graph in text format
    std::string dumpLabel() const;

    /// Returns the indices of `valIter` that satisfy `this` condition.
    void getFilteredIndices(const IteratorPair &valIter,
                            const std::optional<IteratorPair> &idsOption,
                            std::vector<uint64Pair> &indices) const;

    /// The delimiter used in our text format
    static const char delimiter='&';
    /// The delimiter used in SQL
    constexpr static const char delimiterSQL[]=" and ";
};
static const std::vector<FilterInfo::Comparison> comparisonTypes { FilterInfo::Comparison::Less, FilterInfo::Comparison::Greater, FilterInfo::Comparison::Equal};
//---------------------------------------------------------------------------
struct PredicateInfo {
    /// Left
    SelectInfo left;
    /// Right
    SelectInfo right;
    /// The constructor
    PredicateInfo(SelectInfo left, SelectInfo right) : left(left), right(right){};
    /// Dump text format
    std::string dumpText();
    /// Dump label graph in text format
    std::string dumpLabel() const;
    /// Dump SQL
    std::string dumpSQL();

    /// The delimiter used in our text format
    static const char delimiter='&';
    /// The delimiter used in SQL
    constexpr static const char delimiterSQL[]=" and ";
};
//---------------------------------------------------------------------------
class QueryInfo {
    public:
    unsigned queryId;
    /// The relation ids
    std::vector<RelationId> relationIds;
    /// The predicates
    std::vector<PredicateInfo> predicates;
    /// The filters
    std::vector<FilterInfo> filters;
    /// The selections
    std::vector<SelectInfo> selections;
    /// Reset query info
    void clear();

    private:
    /// Parse a single predicate
    void parsePredicate(std::string& rawPredicate);
    /// Resolve bindings of relation ids
    void resolveRelationIds();

    public:
    /// Parse relation ids <r1> <r2> ...
    void parseRelationIds(std::string& rawRelations);
    /// Parse predicates r1.a=r2.b&r1.b=r3.c...
    void parsePredicates(std::string& rawPredicates);
    /// Parse selections r1.a r1.b r3.c...
    void parseSelections(std::string& rawSelections);
    /// Parse selections [RELATIONS]|[PREDICATES]|[SELECTS]
    void parseQuery(std::string& rawQuery);
    /// Dump text format
    std::string dumpText();
    /// Dump SQL
    std::string dumpSQL();


    void getSelectionsMap(std::unordered_map<SelectInfo, unsigned> &selectionsMap) const;


    /// The empty constructor
    QueryInfo() {}
    QueryInfo(unsigned queryId) : queryId(queryId) { }
    /// The constructor that parses a query
    QueryInfo(std::string rawQuery);
};
//---------------------------------------------------------------------------
namespace std {
    template<>
    struct hash<SelectInfo>
    {
        inline size_t operator()(const SelectInfo & s) const
        {
            size_t seed = 0;
            ::hash_combine(seed, s.relId);
            ::hash_combine(seed, s.binding);
            ::hash_combine(seed, s.colId);

            return seed;
        }
    };
}
