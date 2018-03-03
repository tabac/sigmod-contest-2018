#include <vector>
#include <iostream>
#include "Parser.hpp"
#include "Planner.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Planner::addRelation(const char* fileName)
// Loads a relation from disk
{
   this->relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
Relation& Planner::getRelation(unsigned relationId)
// Loads a relation from disk
{
    if (relationId >= this->relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return this->relations[relationId];
}
//---------------------------------------------------------------------------
Plan& Planner::generatePlan(QueryInfo &query)
{
    Plan *p = new Plan();

    return *p;
}
//---------------------------------------------------------------------------
Plan& Planner::generateAllPlans(vector<QueryInfo> queries)
{
    Plan *p = new Plan();

    return *p;
}
//---------------------------------------------------------------------------
