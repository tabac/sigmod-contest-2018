#include <iostream>
#include <cassert>
#include "Mixins.hpp"
#include "Planner.hpp"
#include "Executor.hpp"
#include "DataEngine.hpp"
#include "Index.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
int main(void)
{

    DataEngine engine;

    // Read join relations
    string line;
    unsigned relId = 0;
    while (getline(cin, line) && line != "Done") {
        engine.addRelation(relId, line.c_str());
        ++relId;
    }

    // Preparation phase (not timed)
    // Build histograms, indices,...

    // Do index crazy.
	
	uint64_t *data = engine.getRelation(0).columns[1];
	uint64_t size = engine.getRelation(0).size;

	SortedIndex *index = new SortedIndex(false, data, size);

	if(index->build()) {
			cout << "Index was built successfully!" << endl;
	} else {
			cout << "Index built failed!" << endl;
	}

	delete index;
	return 0;

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

        delete plan;
    }

    return 0;
}
//---------------------------------------------------------------------------
