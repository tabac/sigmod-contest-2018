#pragma once
#include <vector>
#include <cstdint>
#include "Relation.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
class Joiner {
  public:
  /// The relations that might be joined
  std::vector<Relation> relations;
  /// Add relation
  void addRelation(const char* fileName);
  /// Get relation
  Relation& getRelation(unsigned id);
  /// Joins a given set of relations
  void join(QueryInfo& i);
};
//---------------------------------------------------------------------------
