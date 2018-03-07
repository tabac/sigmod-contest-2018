#include <vector>
#include <cstdint>
#include <variant>
#include <iostream>
#include "Executor.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void ResultInfo::printResultInfo()
// Prints the `results` vector to stdout.
{
    vector<variant<uint64_t, bool>>::iterator it;
    for (it = this->results.begin(); it != this->results.end(); ++it) {

        try {
            cout << get<uint64_t>(*it) << " ";
        } catch (const bad_variant_access&) {
            cout << "NULL ";
        }
    }
    cout << endl;
}
//---------------------------------------------------------------------------
void ResultInfo::printResults(vector<ResultInfo> resultsInfo)
// Prints the `ResultInfo` vector to stdout.
{
    vector<ResultInfo>::iterator it;
    for (it = resultsInfo.begin(); it != resultsInfo.end(); ++it) {
        it->printResultInfo();
    }
    cout << endl;
}
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
