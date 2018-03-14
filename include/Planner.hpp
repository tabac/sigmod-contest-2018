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
    /// Generates a plan for a single query
    static Plan* generateSingleQueryPlan(DataEngine &engine, QueryInfo &q);
    /// Generates a plan for the `queries`.
    static Plan* generatePlan(DataEngine &engine, std::vector<QueryInfo> &queries);
    /// Clears the plan and all the supportive structs in memory
    /// Prints the graph of the plan
    static void printPlanGraph(Plan* plan);
    private:
    /// Adds a base-relation datanode in the graph
    static void addBaseRelationNode(RelationId& r, DataEngine& engine, Plan *splan, std::unordered_map<RelationId, vector<SelectInfo>>& columnsToPush,
                  std::unordered_map<RelationId, AbstractNode*>& lastAttached);
    /// Adds a filter operator node in the graph
    static void addFilterNode(FilterInfo& f, Plan *splan, std::unordered_map<RelationId, vector<SelectInfo>>& columnsToPush,
                  std::unordered_map<RelationId, AbstractNode*>& lastAttached);
    /// Adds a join operator node in the graph
    static void addJoinNode(PredicateInfo& p, Plan *splan, std::unordered_map<RelationId, vector<SelectInfo>>& columnsToPush,
                  std::unordered_map<RelationId, AbstractNode*>& lastAttached);
    /// Adds an aggregate-selection operator node in the graph
    static void addAggregateNode(Plan &plan, QueryInfo& query,
                                 std::unordered_map<RelationId, AbstractNode*>& lastAttached);

    static void attachQueryPlan(Plan &plan, DataEngine &engine, QueryInfo &query);

    static void addFilter(Plan &plan, FilterInfo& filter,
                          std::unordered_set<SelectInfo> selections,
                          std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addJoin(Plan& plan, PredicateInfo& predicate,
                        std::unordered_set<SelectInfo> selections,
                        std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addFilterJoin(Plan& plan, PredicateInfo& predicate,
                              std::unordered_set<SelectInfo> selections,
                              std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void addAggregate(Plan &plan, QueryInfo& query,
                             std::unordered_map<unsignedPair, AbstractNode *> &lastAttached);

    static void updateAttached(std::unordered_map<unsignedPair, AbstractNode *> &lastAttached,
                               AbstractNode *oldNode, AbstractNode *newNode);
};
//---------------------------------------------------------------------------
