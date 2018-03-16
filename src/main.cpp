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
	
//	uint64_t *data = engine.getRelation(0).columns[1];
//	uint64_t size = engine.getRelation(0).size;

	// val: 0 1 1 1 1 2 3 3 4 5  6  9
	// idx: 0 1 2 3 4 5 6 7 8 9 10 11
	uint64_t size = 12;
	uint64_t data[size] = {1,5,6,9,2,1,4,0,1,3,1,3};

	SortedIndex *index = new SortedIndex(data, size);

//	cout << index->findElement(0) << endl; // must return 0
//	cout << index->findElement(1) << endl; // must return 1
//	cout << index->findElement(9) << endl; // must return 11
//	cout << index->findElement(8) << endl; // must return 10
//	cout << index->findElement(3) << endl; // must return 6
//	cout << index->findElement(100000) << endl; // must return 11

	SelectInfo sel = SelectInfo(0,1,2);
	FilterInfo *filterInfo = new FilterInfo(sel, 0, FilterInfo::Comparison::Greater);
	IteratorPair it = index->getValuesIterator(sel, filterInfo);
	for(vector<uint64_t>::iterator i=it.first; i!=it.second;i++) {
		cout<< (*i) << ", ";
	}
	cout<< endl;

	delete filterInfo;

	filterInfo = new FilterInfo(sel, 10, FilterInfo::Comparison::Less);
	it = index->getValuesIterator(sel, filterInfo);
	for(vector<uint64_t>::iterator i=it.first; i!=it.second;i++) {
		cout<< (*i) << ", ";
	}
	cout<< endl;
	delete filterInfo;

	filterInfo = new FilterInfo(sel, 3, FilterInfo::Comparison::Equal);
	it = index->getValuesIterator(sel, filterInfo);
	for(vector<uint64_t>::iterator i=it.first; i!=it.second;i++) {
		cout<< (*i) << ", ";
	}
	cout<< endl;
	delete filterInfo;
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
