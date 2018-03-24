#pragma once
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "Plan.hpp"
#include "Parser.hpp"

class Planner {
    public:
    static void attachQueryPlan(Plan &plan, QueryInfo &query);

    static void addFilter(Plan &plan, FilterInfo& filter,
                          std::unordered_set<SelectInfo> &selections,
                          std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addJoin(Plan& plan, PredicateInfo& predicate,
                        std::unordered_set<SelectInfo> &selections,
                        std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addFilterJoin(Plan& plan, PredicateInfo& predicate,
                              std::unordered_set<SelectInfo> &selections,
                              std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addAggregate(Plan &plan, QueryInfo& query,
                             std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void updateAttached(std::unordered_map<unsignedPair, AbstractNode *> &lastAttached,
                               unsignedPair relationPair, AbstractNode *newNode);

    static Plan* generatePlan(std::vector<QueryInfo> &queries);

    static void setSelections(SelectInfo &selection,
                              std::unordered_set<SelectInfo> &selections,
                              AbstractNode *node);

    static bool filterComparator(const FilterInfo& f1, const FilterInfo& f2);

    static bool predicateComparator(const PredicateInfo& p1, const PredicateInfo& p2);

#ifndef NDEBUG
    static void printPlan(Plan* plan);

    static void printAttached(std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);
#endif

};
//---------------------------------------------------------------------------
