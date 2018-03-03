#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Parser.hpp"

using namespace std;
//---------------------------------------------------------------------------
void Joiner::addRelation(const char* fileName)
// Loads a relation from disk
{
   relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
Relation& Joiner::getRelation(unsigned relationId)
// Loads a relation from disk
{
    if (relationId >= relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return relations[relationId];
}
//---------------------------------------------------------------------------
void Joiner::join(QueryInfo &q)
// Hashes a value and returns a check-sum
// The check should be NULL if there is no qualifying tuple
{
    cout << "---" << endl; vector<RelationId>::iterator it;
    for (it = q.relationIds.begin(); it != q.relationIds.end(); ++it) {
        cout <<  *it << endl;
    }
}
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    Joiner joiner;

    // Read join relations
    string line;
    while (getline(cin, line) && line != "Done") {
        joiner.addRelation(line.c_str());
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
        vector<QueryInfo>::iterator it;
        for (it = queries.begin(); it != queries.end(); ++it) {
            joiner.join(*it);
        }

        // Clear query vector before loading next query batch.
        queries.clear();
    }

    return 0;
}
