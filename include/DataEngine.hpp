#pragma once
#include <mutex>
#include <vector>
#include <cstdint>
#include "Relation.hpp"
//---------------------------------------------------------------------------
class DataEngine {
    public:
    /// All available relations.
    std::vector<Relation> relations;

    /// Loads a relations from disk.
    void addRelation(RelationId relId, const char* fileName);
    /// Returns a reference to a `Relation` instance by id.
    Relation& getRelation(unsigned id);
    /// Returns the estimated selectivity of operator `AbstractOperatorNode` on dataset `DataNode`
    float getEstimatedSelectivity(AbstractOperatorNode &op, DataNode &d);

    void createSortedIndexes(void);

    private:
    /// Estimates the selectivity of filter operator `FilterOperatorNode` on dataset `DataNode `
    float getFilterSelectivity(FilterOperatorNode* filterOp, DataNode &d);
    /// Estimates the selectivity of join operator `JoinOperatorNode` on dataset `DataNode `
    float getJoinSelectivity(JoinOperatorNode* filterOp, DataNode &d);
};
//---------------------------------------------------------------------------
