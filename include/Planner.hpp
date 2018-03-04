#pragma once
#include <vector>
#include "Plan.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class Planner {
    public:
    /// All available relations.
    vector<Relation> relations;

    /// Loads a relation from disk.
    void addRelation(const char* fileName);
    /// Returns a reference to a Relation by id.
    Relation& getRelation(unsigned id);

    /// Generates a plan for query `QueryInfo`.
    static Plan &generatePlan(QueryInfo &query);
    /// Generates all plans for queries in `vector<QueryInfo>`.
    static Plan &generateAllPlans(vector<QueryInfo> queries);
};
//---------------------------------------------------------------------------
