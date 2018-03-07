#include <iostream>
#include "Planner.hpp"
#include "Executor.hpp"
#include "DataEngine.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
int main(void)
{
    DataEngine engine;

    // Read join relations
    string line;
    while (getline(cin, line) && line != "Done") {
        engine.addRelation(line.c_str());
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

        // Reserve memory for results if necessary.
        resultsInfo.reserve(queries.size());

        // Generate an execution plan for all `queries`.
        Plan *plan = Planner::generatePlan(engine, queries);

        // Execute the generated plan.
        Executor::executePlan(*plan, resultsInfo);

        // Print results.
        ResultInfo::printResults(resultsInfo);

        // Clear info before loading next query batch.
        resultsInfo.clear();
        queries.clear();
        // Delete created plan.
        delete plan;
    }

    return 0;
}
