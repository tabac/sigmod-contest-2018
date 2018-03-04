#include <iostream>
#include "Planner.hpp"
#include "Executor.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    Planner planner;
    Executor executor;

    // Read join relations
    string line;
    while (getline(cin, line) && line != "Done") {
        planner.addRelation(line.c_str());
    }

    // Preparation phase (not timed)
    // Build histograms, indices,...

    // Do index crazy.

    // The test harness will send the first query after 1 second.

    vector<QueryInfo> queries;
    vector<ResultInfo> resultsInfo;
    while (getline(cin, line) && line != "F") {
        // Load next query batch.
        do {
            queries.emplace_back().parseQuery(line);
        } while (getline(cin, line) && line != "F");

        // Generate an execution plan for all `queries`.
        Plan &plan = planner.generateAllPlans(queries);

        // Execute the generated plan.
        resultsInfo = executor.executePlan(plan);

        // Print results.
        ResultInfo::printResults(resultsInfo);

        // Clear info before loading next query batch.
        resultsInfo.clear();
        queries.clear();
    }

    return 0;
}
