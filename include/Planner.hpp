#pragma once
#include <vector>
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
    /// Prints the graph of the plan
    static void printPlanGraph(Plan* plan);
    static bool nodeMatching(vector<AbstractNode *> v, string label);
};
//---------------------------------------------------------------------------
