#include <vector>
#include <cstdint>
#include <variant>
#include "Executor.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Executor::executePlan(Plan &plan, vector<ResultInfo> &resultsInfo)
// Executes a `Plan`.
{
    resultsInfo.emplace_back();
    resultsInfo.emplace_back();
    resultsInfo.emplace_back();

    variant<uint64_t, bool> v;
    for (uint64_t i = 1; i < 4; ++i) {
        v = i;
        resultsInfo[0].results.push_back(v);
    }
    for (uint64_t i = 4; i < 6; ++i) {
        v = i;
        resultsInfo[1].results.push_back(v);
    }
    for (uint64_t i = 6; i < 9; ++i) {
        v = i;
        resultsInfo[2].results.push_back(v);
    }
}
//---------------------------------------------------------------------------
void Executor::executeOperator(AbstractOperatorNode *node)
// Executes the operator of an `AbstractOperatorNode`.
{
    node->execute();
}
//---------------------------------------------------------------------------
