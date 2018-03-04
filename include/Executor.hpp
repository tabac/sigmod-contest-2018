#pragma once
#include <vector>
#include <cstdint>
#include "Plan.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class ResultInfo {
    public:
    /// Query results.
    vector<uint64_t> results;

    /// Prints the `ResultInfo` vector to stdout.
    static void printResults(vector<ResultInfo> resultsInfo);
};
//---------------------------------------------------------------------------
class Executor {
    public:
    /// Executes a `Plan`.
    static vector<ResultInfo> executePlan(Plan &plan);
    /// Executes the operator of an `OperatorPlanNode`.
    static void executeOperator(Plan &plan, OperatorPlanNode &node);
};
//---------------------------------------------------------------------------
