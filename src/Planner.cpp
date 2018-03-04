#include <vector>
#include <iostream>
#include "Parser.hpp"
#include "Planner.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Planner::addRelation(const char* fileName)
// Loads a relation from disk.
{
   this->relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
Relation& Planner::getRelation(unsigned relationId)
// Returns a reference to a Relation by id.
{
    if (relationId >= this->relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return this->relations[relationId];
}
//---------------------------------------------------------------------------
Plan& Planner::generatePlan(QueryInfo &query)
// Generates a plan for query `QueryInfo`.
{
    Plan *p = new Plan();

    return *p;
}
//---------------------------------------------------------------------------
Plan& Planner::generateAllPlans(vector<QueryInfo> queries)
// Generates all plans for queries in `vector<QueryInfo>`.
{
    Plan *p = new Plan();

    return *p;
}
//---------------------------------------------------------------------------
