#pragma once
#include <vector>
#include <variant>
#include <cstdint>
#include "Plan.hpp"
//---------------------------------------------------------------------------
class Executor {
    public:
    /// Executes the given `Plan`, stores results in `resultsInfo`.
    static void executePlan(Plan &plan, std::vector<ResultInfo> &resultsInfo);
    /// Executes the operator of an `OperatorNode`.
    static void executeOperator(AbstractNode *node);
};
//---------------------------------------------------------------------------

