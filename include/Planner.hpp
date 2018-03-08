#pragma once
#include <vector>
#include "Plan.hpp"
#include "Parser.hpp"
#include "DataEngine.hpp"
//---------------------------------------------------------------------------
class Planner {
    public:
    /// Generates a plan for the `queries`.
    static Plan* generatePlan(DataEngine &engine, std::vector<QueryInfo> &queries);
};
//---------------------------------------------------------------------------
