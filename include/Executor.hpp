#pragma once
#include <vector>
#include <cstdint>
#include "Plan.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
struct ResultInfo {
    /// Query results ordered by `QueryInfo` index.
    vector<vector<uint64_t>> results;
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
