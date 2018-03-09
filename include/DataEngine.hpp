#pragma once
#include <vector>
#include <cstdint>
#include "Relation.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class DataEngine {
  public:
  /// All available relations.
  vector<Relation> relations;

  /// Loads a relations from disk.
  void addRelation(RelationId relId, const char* fileName);
  /// Returns a reference to a `Relation` instance by id.
  Relation& getRelation(unsigned id);
};
//---------------------------------------------------------------------------
