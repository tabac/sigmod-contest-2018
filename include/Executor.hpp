#pragma once
#include <vector>
#include <variant>
#include <cstdint>
#include "Plan.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class ResultInfo {
    public:
    /// Query results.
    vector<variant<uint64_t, bool>> results;

    /// Prints the `results` vector to stdout.
    void printResultInfo();
    /// Prints the `ResultInfo` vector to stdout.
    static void printResults(vector<ResultInfo> resultsInfo);
};
//---------------------------------------------------------------------------
class Executor {
    public:
    /// Executes the given `Plan`, stores results in `resultsInfo`.
    static void executePlan(Plan &plan, vector<ResultInfo> &resultsInfo);
    /// Executes the operator of an `OperatorNode`.
    static void executeOperator(AbstractOperatorNode *node);
};
//---------------------------------------------------------------------------

