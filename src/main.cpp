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
        DataEngine::buildCompleteHist(relId, 100, 10);
        ++relId;
    }


    // Preparation phase (not timed)
    // Build histograms, indices,...

    // ------------ test histograms ---------------------------

//    Histogram& h = *new Histogram(engine.relations[0], 6, 4000/100);
//    delete &h;

//    Histogram& h = engine.histograms.at(HistKey (13,6));
//    cout << "Result: " << h.getEstimatedKeys(7254, 8120) << endl;
//    cout << "Result: " << h.getEstimatedKeys(4000, 10000) << endl;

//    return 0;

    // ------------ test histograms ---------------------------

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

        cout << "NEW BATCH" << endl;
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
//    for(unordered_map<HistKey, Histogram*>::iterator itr = DataEngine::histograms.begin();
//        itr != DataEngine::histograms.end(); itr++) {
//
//        delete itr->second;
//    }
//    vector<Relation>().swap(DataEngine::relations);
//    unordered_map<HistKey, Histogram*>().swap(DataEngine::histograms);


    return 0;
}
//---------------------------------------------------------------------------
