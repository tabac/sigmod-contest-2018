#include <iostream>
#include <cassert>
#include "Mixins.hpp"
#include "Planner.hpp"
#include "Executor.hpp"
#include "DataEngine.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void doAssertions(DataEngine &engine);
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

    doAssertions(engine);

    // Preparation phase (not timed)
    // Build histograms, indices,...

    // Do index crazy.

    // The test harness will send the first query after 1 second.

    vector<QueryInfo> queries;
    vector<ResultInfo> resultsInfo;
    while (getline(cin, line) && line != "F") {
        // Load next query batch.
        do {
            cout << line << endl;
            queries.emplace_back().parseQuery(line);
        } while (getline(cin, line) && line != "F");

        // Reserve memory for results if necessary.
        resultsInfo.reserve(queries.size());
        cout << "Ready for the planner" << endl;
        // Generate an execution plan for all `queries`.
        Plan *plan = Planner::generatePlan(engine, queries);
        cout << "Plan is finished" << endl;
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

void doAssertions(DataEngine &engine)
// TODO: This is only for debug, should be deleted.
{
    assert(engine.relations.size() == 14);

    assert(engine.relations[0].size == 1561);

    assert(engine.relations[0].columns.size() == 3);

    assert(engine.relations[0].ids.size() == 1561);

    assert(engine.relations[0].ids[1560] == 1560);

    SelectInfo selectInfo (0, 0, 2);
    IteratorPair idsIter = engine.relations[0].getIdsIterator(selectInfo, NULL);
    uint64_t i = 0;
    vector<uint64_t>::iterator it = idsIter.first;
    for ( ; it != idsIter.second; ++i, ++it) {
        assert(i == (*it));
    }
    assert(i == 1561);

    optional<IteratorPair> option = engine.relations[0].getValuesIterator(selectInfo, NULL);
    assert(option.has_value());
    IteratorPair firstCol = option.value();

    for (i = 0, it = firstCol.first; it != firstCol.second; ++it, ++i) {
        assert((*it) == engine.relations[0].columns[2][i]);
    }

    assert(i == 1561);

    cout << "OK!" << endl;
}
