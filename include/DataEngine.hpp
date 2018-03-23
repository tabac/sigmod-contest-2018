#pragma once
#include <vector>
#include <unordered_map>
#include <cstdint>
#include "Relation.hpp"
#include "Histogram.hpp"

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
    ~DataEngine();
  /// All available relations.
  std::vector<Relation> relations;
  /// All available histograms indexed by relation and column ID
  HistCatalog histograms;

    /// Build a histogram with `numOfBuckets` buckets for all columns of relation `r`.
    /// A sample ratio `s`, builds the histogram on the 1/s of the initial relation
    void buildCompleteHist(RelationId r, int sampleRatio, int numOfBuckets);

  /// Loads a relations from disk.
  void addRelation(RelationId relId, const char* fileName);
  /// Returns a reference to a `Relation` instance by id.
  Relation& getRelation(unsigned id);
  /// Returns the estimated selectivity of operator `AbstractOperatorNode` on dataset `DataNode`
  //uint64_t getEstimatedSelectivity(AbstractOperatorNode &op, DataNode &d);
  /// Estimates the selectivity of filter operator `FilterOperatorNode` on dataset `DataNode `
  uint64_t getFilterSelectivity(FilterInfo& filter);
    /// Estimates the selectivity of join operator `JoinOperatorNode` on dataset `DataNode `
    uint64_t getJoinSelectivity(PredicateInfo& predicate);

//  private:
//  /// Estimates the selectivity of filter operator `FilterOperatorNode` on dataset `DataNode `
//  uint64_t getFilterSelectivity(FilterOperatorNode* filterOp, DataNode &d);
//  /// Estimates the selectivity of join operator `JoinOperatorNode` on dataset `DataNode `
//  uint64_t getJoinSelectivity(JoinOperatorNode* filterOp, DataNode &d);

};
//---------------------------------------------------------------------------
