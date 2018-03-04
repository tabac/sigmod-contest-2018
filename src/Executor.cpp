#include <vector>
#include <cstdint>
#include <iostream>
#include "Executor.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void ResultInfo::printResults(vector<ResultInfo> resultsInfo)
// Prints the `ResultInfo` vector to stdout.
{
    vector<ResultInfo>::iterator it;
    for (it = resultsInfo.begin(); it != resultsInfo.end(); ++it) {
        vector<uint64_t>::iterator jt;
        for (jt = it->results.begin(); jt != it->results.end(); ++jt) {
            cout << *jt << " ";
        }
        cout << endl;
    }
    cout << endl;
}
//---------------------------------------------------------------------------
vector<ResultInfo> Executor::executePlan(Plan &plan)
// Executes a `Plan`.
{
    vector<ResultInfo> v;

    v.emplace_back();
    v.emplace_back();
    v.emplace_back();

    v[0].results.push_back(1);
    v[0].results.push_back(2);
    v[0].results.push_back(3);

    v[1].results.push_back(4);
    v[1].results.push_back(5);

    v[2].results.push_back(6);
    v[2].results.push_back(7);
    v[2].results.push_back(8);

    return v;
}
//---------------------------------------------------------------------------
void Executor::executeOperator(Plan &plan, OperatorPlanNode &node)
// Executes the operator of an `OperatorPlanNode`.
{
}
//---------------------------------------------------------------------------
