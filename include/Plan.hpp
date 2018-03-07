#pragma once
#include <vector>
#include <cstdint>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class AbstractNode {
    public:
    ///In-edge adjacency list.
    vector<AbstractNode *> inAdjacencyList;
    ///Out-edge adjacency list.
    vector<AbstractNode *> outAdjacencyList;
};
//---------------------------------------------------------------------------
class DataNode : public AbstractNode {
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
class AbstractOperatorNode : public AbstractNode {
    public:
    virtual void execute() = 0;
};
//---------------------------------------------------------------------------
class Plan {
    public:
    /// All the nodes of the plan.
    vector<AbstractNode *> nodes;
};
//---------------------------------------------------------------------------
