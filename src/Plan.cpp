#include <vector>
#include <variant>
#include <cstdint>
#include <iostream>
#include "Plan.hpp"
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
void DataNode::execute()
{
}
//---------------------------------------------------------------------------
ResultInfo DataNode::aggregate()
{
}
//---------------------------------------------------------------------------
void JoinOperatorNode::execute()
{
}
//---------------------------------------------------------------------------
void FilterOperatorNode::execute()
{
}
//---------------------------------------------------------------------------
Plan::~Plan()
{
    vector<AbstractNode *>::iterator it;
    for (it = this->nodes.begin(); it != this->nodes.end(); ++it) {
        delete (*it);
    }
}
//---------------------------------------------------------------------------
