#include <iostream>
#include "DataEngine.hpp"
#include "Planner.hpp"
#include "Executor.hpp"
//---------------------------------------------------------------------------
using namespace std;
vector<Relation> DataEngine::relations;
HistCatalog DataEngine::histograms;
//---------------------------------------------------------------------------
int main(void)
{
    //DataEngine engine;

    // Read join relations
    string line;
    unsigned relId = 0;
    while (getline(cin, line) && line != "Done") {
        DataEngine::addRelation(relId, line.c_str());
        // relation, 1/samplingRatio, buckets
        DataEngine::buildCompleteHist(relId, 100, 200);
        ++relId;
    }

    // Preparation phase (not timed)
    // Build histograms, indices,...

    // Do index crazy.

    // The test harness will send the first query after 1 second.

    vector<QueryInfo> queries;
    vector<ResultInfo> resultsInfo;
    while (getline(cin, line) && line != "F") {
        // Load next query batch.
        unsigned q = 0;
        do {
            queries.emplace_back(q++).parseQuery(line);
        } while (getline(cin, line) && line != "F");

#ifndef NDEBUG
        cout << "NEW BATCH" << endl;
#endif
        // Reserve memory for results if necessary.
        resultsInfo.reserve(queries.size());
        // Generate an execution plan for all `queries`.
        Plan *plan = Planner::generatePlan(queries);
        // Execute the generated plan.
        Executor::executePlan(*plan, resultsInfo);

        // Print results.
        ResultInfo::printResults(resultsInfo);

        // Clear info before loading next query batch.
        resultsInfo.clear();
        queries.clear();

        delete plan;
    }


    // clear histogram pointers
    for(unordered_map<HistKey, Histogram*>::iterator itr = DataEngine::histograms.begin();
        itr != DataEngine::histograms.end(); itr++) {

        delete itr->second;
    }

    return 0;
}
//---------------------------------------------------------------------------
