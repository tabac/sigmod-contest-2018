#pragma once
#include <vector>
#include <cstdint>
#include "Mixins.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
enum NodeStatus { fresh, processing, processed };
//---------------------------------------------------------------------------
class ResultInfo {
    public:
    /// Query results.
    std::vector<std::optional<uint64_t>> results;

    /// Prints the `results` vector to stdout.
    void printResultInfo();
    /// Prints the `ResultInfo` vector to stdout.
    static void printResults(std::vector<ResultInfo> resultsInfo);
};
//---------------------------------------------------------------------------
class AbstractNode {
    public:
    /// Status of the current node.
    NodeStatus status;
    /// Used by the `Executor` when performing BFS.
    unsigned visited;
    /// In-edge adjacency list.
    std::vector<AbstractNode *> inAdjList;
    /// Out-edge adjacency list.
    std::vector<AbstractNode *> outAdjList;

    /// Executes node-type related functionality.
    virtual void execute() = 0;

    /// Status setter.
    void setStatus(NodeStatus status) { this->status = status; };

    /// Status getters.
    bool isStatusFresh() { return status == fresh; };
    bool isStatusProcessing() { return status == processing; };
    bool isStatusProcessed() { return status == processed; };

    /// Constructor.
    AbstractNode() : status(fresh), visited(0) { }
    /// Virtual destructor.
    virtual ~AbstractNode() { }

    /////////////////////////////////////////////////////////////////////////
    /// FOR DEBUG PURPOSES
    unsigned nodeId;
    void setNodeId(unsigned nodeId) { this->nodeId = nodeId; };
    /////////////////////////////////////////////////////////////////////////
};
//---------------------------------------------------------------------------
class AbstractDataNode : public AbstractNode {
    public:
    /// Returns the relations columns aggregated sums.
    virtual ResultInfo aggregate() = 0;
};
//---------------------------------------------------------------------------
class DataNode : public AbstractDataNode, public DataReaderMixin {
    public:
    /// The number of tuples (rows).
    uint64_t size;
    /// A vector of `SelectInfo` instances of the columns
    /// appearing in the `data` vector.
    std::vector<SelectInfo *> columns;
    /// A table in columnar format with "value" entries.
    std::vector<uint64_t> dataValues;
    /// A table in columnar format with "row ID" entries.
    std::vector<uint64_t> dataIds;

    /// Checks if the nodes it depends on are `processed`
    /// and if so sets its flag to processed too.
    void execute();
    /// Returns the relations columns aggregated sums.
    // TODO: This should return something else.
    ResultInfo aggregate();
    /// Returns an `IteratorPair` over all the `DataNode`'s ids.
    /// Ignores `filterInfo`, requires it being `NULL`.
    IteratorPair getIdsIterator(FilterInfo* filterInfo);
    /// Returns an `IteratorPair` over all the `DataNode`'s values
    /// of the column specified by `selectInfo`.
    /// Ignores `filterInfo`, requires it being `NULL`.
    IteratorPair getValuesIterator(SelectInfo& selectInfo, FilterInfo* filterInfo);
    /// Destructor.
    ~DataNode() { }
};
//---------------------------------------------------------------------------
class AbstractOperatorNode : public AbstractNode { };
//---------------------------------------------------------------------------
class JoinOperatorNode : public AbstractOperatorNode {
    public:
    /// Reference to the corresponding `PredicateInfo` instance.
    const struct PredicateInfo &info;

    /// Joins the two input `DataNode` instances.
    void execute();

    JoinOperatorNode(struct PredicateInfo &info) : info(info) {}

    ~JoinOperatorNode() { }
};
//---------------------------------------------------------------------------
class FilterOperatorNode : public AbstractOperatorNode {
    public:
    /// Reference to the corresponding `FilterInfo` instance.
    const struct FilterInfo &info;

    /// Filters the input `DataNode` instance.
    void execute();

    /*
    /// Find row indexes in `data` that satisfy the filter condition.
    std::vector<unsigned> filterDataByColIndex(unsigned colIndex);
    */

    FilterOperatorNode(struct FilterInfo &info) : info(info) {}

    ~FilterOperatorNode() { }
};
//---------------------------------------------------------------------------
class Plan {
    public:
    /// A pointer to the `root` node, the beginning of the
    /// execution plan(s) graph.
    AbstractNode *root;
    /// Pointers to all the nodes of the plan(s).
    std::vector<AbstractNode *> nodes;
    /// All the exit nodes of the plan(s).
    std::vector<DataNode *> exitNodes;

    ~Plan();
};
//---------------------------------------------------------------------------
