#pragma once
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "Plan.hpp"
#include "Parser.hpp"

using OriginTracker = std::unordered_map<unsignedPair, AbstractNode *>;
using JoinCatalog = std::unordered_map<PredicateInfo, JoinOperatorNode *>;

//template <class T>
//inline void hash_combine(std::size_t& seed, const T& v)
//{
//    std::hash<T> hasher;
//    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
//}


class Planner {
    public:

    static void attachQueryPlan(Plan &plan, QueryInfo &query);

    static OriginTracker connectQueryBaseRelations(Plan &plan, QueryInfo &query);

    static JoinCatalog findCommonJoins(std::vector<QueryInfo> &batch);

    static unsigned addFilters(Plan &plan, QueryInfo& query, OriginTracker &lastAttached);

    static void addFilters2(Plan &plan, QueryInfo& query, OriginTracker &lastAttached);


    static void addRemainingFilters(Plan &plan, QueryInfo& query, unsigned pft, OriginTracker &lastAttached);

    static void addJoins(Plan& plan, QueryInfo& query, OriginTracker &lastAttached);

    static void addJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query, OriginTracker &lastAttached);

    static void addFilterJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query, OriginTracker &lastAttached);

    static void addAggregate(Plan &plan, const QueryInfo& query, OriginTracker &lastAttached);

    static void updateAttached(OriginTracker &lastAttached, const unsignedPair relationPair, AbstractNode *newNode);

    static Plan* generatePlan(std::vector<QueryInfo> &queries);

    static void setQuerySelections(Plan &plan, QueryInfo &query);

    static bool filterComparator(const FilterInfo& f1, const FilterInfo& f2);

    static bool predicateComparator(const PredicateInfo& p1, const PredicateInfo& p2);

#ifndef NDEBUG
    static void printPlan(Plan* plan);

    static void printAttached(OriginTracker &lastAttached);
#endif

};
//---------------------------------------------------------------------------
