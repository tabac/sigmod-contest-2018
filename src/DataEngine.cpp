#include <vector>
#include <iostream>
#include "DataEngine.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void DataEngine::addRelation(RelationId relId, const char* fileName)
// Loads a relation from disk
{
   this->relations.emplace_back(relId, fileName);
}
//---------------------------------------------------------------------------
Relation& DataEngine::getRelation(unsigned relationId)
// Loads a relation from disk
{
   if (relationId >= this->relations.size()) {
      cerr << "Relation with id: " << relationId << " does not exist" << endl;
      throw;
   }
   return this->relations[relationId];
}
//---------------------------------------------------------------------------
