#pragma once
#include <vector>
#include "Plan.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class Planner {
    public:
    /// Generates plan for query `QueryInfo`.
    static Plan &generatePlan(QueryInfo &query);
    /// Generates all plans for queries in `vector<QueryInfo>`.
    static Plan &generateAllPlans(vector<QueryInfo> queries);
};
//---------------------------------------------------------------------------
