#pragma once
#include <vector>
#include <cstdint>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class AbstractPlanNode {
    public:
    ///In-edge adjacency list.
    vector<AbstractPlanNode *> inAdjacencyList;
    ///Out-edge adjacency list.
    vector<AbstractPlanNode *> outAdjacencyList;
};
//---------------------------------------------------------------------------
class DataPlanNode : public AbstractPlanNode {
    public:
    /// A vector of pairs of `{relationId,numberOfColumns}` entries
    /// appearing in the `data` vector.
    vector<pair<uint64_t, uint64_t>> relations;
    /// A vector of column Ids appearing in the `data` vector.
    vector<uint64_t> columns;
    /// A table in columnar format with `{rowId, rowValue}` entries.
    vector<pair<uint64_t, uint64_t>> data;
};
//---------------------------------------------------------------------------
class OperatorPlanNode : public AbstractPlanNode {
    virtual void execute() = 0;
};
//---------------------------------------------------------------------------
class Plan {
    public:
    /// All the nodes of the plan.
    vector<AbstractPlanNode> nodes;
};
//---------------------------------------------------------------------------
