#include <iostream>
#include "Planner.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    Planner planner;

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
    while (getline(cin, line) && line != "F") {
        // Load next query batch.
        do {
            queries.emplace_back().parseQuery(line);
        } while (getline(cin, line) && line != "F");

        // Process next query batch.

        // Clear query vector before loading next query batch.
        queries.clear();
    }

    return 0;
}
