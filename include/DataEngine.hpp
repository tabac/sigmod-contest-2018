#pragma once
#include <vector>
#include <unordered_map>
#include <cstdint>
#include "Relation.hpp"
#include "Histogram.hpp"
#include "Parser.hpp"

//---------------------------------------------------------------------------
using HistKey = std::pair<RelationId, unsigned>;

namespace std {
    template <>
    struct hash<HistKey> {
        size_t operator ()(HistKey relationColumnPair) const {
            auto h1 = std::hash<RelationId>{}(relationColumnPair.first);
            auto h2 = std::hash<unsigned>{}(relationColumnPair.second);
            return h1 ^ h2;
        }
    };
}

using HistCatalog = std::unordered_map<HistKey, Histogram*>;
//---------------------------------------------------------------------------
class DataEngine {

  public:

    //~DataEngine();
  /// All available relations.
  static std::vector<Relation> relations;
  /// All available histograms indexed by relation and column ID
  static HistCatalog histograms;

    /// Build a histogram with `numOfBuckets` buckets for all columns of relation `r`.
    /// A sample ratio `s`, builds the histogram on the 1/s of the initial relation
  static void buildCompleteHist(RelationId r, int sampleRatio, int numOfBuckets);

  /// Loads a relations from disk.
  static void addRelation(RelationId relId, const char* fileName);
  /// Returns a reference to a `Relation` instance by id.
  static Relation& getRelation(unsigned id);
  /// Estimates the selectivity of FilterInfo `filter`
  static uint64_t getFilterSelectivity(const FilterInfo& filter);
    /// Estimates the selectivity of PredicateInfo `predicate`
  static uint64_t getJoinSelectivity(const PredicateInfo& predicate);

//  private:
//  /// Estimates the selectivity of filter operator `FilterOperatorNode` on dataset `DataNode `
//  uint64_t getFilterSelectivity(FilterOperatorNode* filterOp, DataNode &d);
//  /// Estimates the selectivity of join operator `JoinOperatorNode` on dataset `DataNode `
//  uint64_t getJoinSelectivity(JoinOperatorNode* filterOp, DataNode &d);

};
//---------------------------------------------------------------------------
