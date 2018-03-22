#pragma once
#include <vector>
#include <unordered_map>
#include <cstdint>
#include "Relation.hpp"
#include "Histogram.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class DataEngine {
  
  public:
  /// All available relations.
  vector<Relation> relations;
  /// All available histograms indexed by relation and column ID
  unordered_map<pair<RelationId, unsigned>, Histogram> histograms;

    /// Build a histogram with `numOfBuckets` buckets for all columns of relation `r`.
    /// A sample ratio `s`, builds the histogram on the 1/s of the initial relation
    void buildCompleteHist(RelationId r, int sampleRatio, int numOfBuckets);

  /// Loads a relations from disk.
  void addRelation(RelationId relId, const char* fileName);
  /// Returns a reference to a `Relation` instance by id.
  Relation& getRelation(unsigned id);
  /// Returns the estimated selectivity of operator `AbstractOperatorNode` on dataset `DataNode`
  float getEstimatedSelectivity(AbstractOperatorNode &op, DataNode &d);

  private:
  /// Estimates the selectivity of filter operator `FilterOperatorNode` on dataset `DataNode `
  float getFilterSelectivity(FilterOperatorNode* filterOp, DataNode &d);
  /// Estimates the selectivity of join operator `JoinOperatorNode` on dataset `DataNode `
  float getJoinSelectivity(JoinOperatorNode* filterOp, DataNode &d);
};
//---------------------------------------------------------------------------
