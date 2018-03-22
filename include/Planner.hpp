#pragma once
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "Plan.hpp"
#include "Parser.hpp"
#include "DataEngine.hpp"
//---------------------------------------------------------------------------
class Planner {
    public:
    static void attachQueryPlan(Plan &plan, const DataEngine &engine, QueryInfo &query);

    static void addFilter(Plan &plan, FilterInfo& filter, const QueryInfo& query,
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

    static Plan* generatePlan(const DataEngine &engine, std::vector<QueryInfo> &queries);

    static void setQuerySelections(Plan &plan, QueryInfo &query);

#ifndef NDEBUG
    static void printPlan(Plan* plan);

    static void printAttached(unordered_map<unsignedPair, AbstractNode *> &lastAttached);
#endif

};
//---------------------------------------------------------------------------
