#pragma once
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "Plan.hpp"
#include "Parser.hpp"

class Planner {
    public:

    static void attachQueryPlan(Plan &plan, QueryInfo &query);

    static unsigned addFilters(Plan &plan, QueryInfo& query,
                           std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addFilters2(Plan &plan, QueryInfo& query,
                               std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);


    static void addRemainingFilters(Plan &plan, QueryInfo& query, unsigned pft,
                                      std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addJoins(Plan& plan, QueryInfo& query,
                         std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query,
                        std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addFilterJoin(Plan& plan, PredicateInfo& predicate, const QueryInfo& query,
                              std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addAggregate(Plan &plan, const QueryInfo& query,
                             std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void updateAttached(std::unordered_map<unsignedPair, AbstractNode *> &lastAttached,
                               const unsignedPair relationPair,
                               AbstractNode *newNode);

    static Plan* generatePlan(std::vector<QueryInfo> &queries);

    static void setQuerySelections(Plan &plan, QueryInfo &query);

    static bool filterComparator(const FilterInfo& f1, const FilterInfo& f2);

    static bool predicateComparator(const PredicateInfo& p1, const PredicateInfo& p2);

#ifndef NDEBUG
    static void printPlan(Plan* plan);

    static void printAttached(std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);
#endif

};
//---------------------------------------------------------------------------
