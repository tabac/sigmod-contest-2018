#pragma once
#include <vector>
#include <cstdint>
#include <cassert>
#include <optional>
#include "Mixins.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
enum NodeStatus { fresh, processing, processed };
//---------------------------------------------------------------------------
class ResultInfo {
    public:
    /// Query results.
    std::vector<std::optional<uint64_t>> results;

    ResultInfo(std::vector<uint64_t> results, unsigned size);
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

    /// Resets the nodes status, adjacency lists.
    /// Used for the relations that are reused between
    /// query batches.
    void resetStatus();

    /// Frees any resources allocated by the node.
    virtual void freeResources() = 0;

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
class AbstractDataNode : public AbstractNode, public DataReaderMixin {
    public:
    /// A vector of `SelectInfo` instances of the columns
    /// appearing in the `data` vector.
    std::vector<SelectInfo> columnsInfo;

    /// Should return the size, that is the number of tuples.
    virtual uint64_t getSize() = 0;
};
//---------------------------------------------------------------------------
class DataNode : public AbstractDataNode {
    public:
    /// The number of tuples (rows).
    uint64_t size;
    /// A table in columnar format with "value" entries.
    std::vector<uint64_t> dataValues;
    /// A table in columnar format with "row ID" entries.
    std::vector<uint64_t> dataIds;

    /// Checks if the nodes it depends on are `processed`
    /// and if so sets its flag to processed too.
    void execute();
    /// Frees any resources allocated by the node.
    void freeResources() { this->dataValues.clear(); this->dataIds.clear(); this->columnsInfo.clear(); }

    /// Returns an `IteratorPair` over all the `DataNode`'s ids.
    /// Ignores `filterInfo`, requires it being `NULL`.
    IteratorPair getIdsIterator(SelectInfo& selectInfo, FilterInfo* filterInfo);
    /// Returns an `IteratorPair` over all the `DataNode`'s values
    /// of the column specified by `selectInfo`.
    /// Ignores `filterInfo`, requires it being `NULL`.
    std::optional<IteratorPair> getValuesIterator(SelectInfo& selectInfo,
                                                  FilterInfo* filterInfo);

    /// Returns the size, that is the number of tuples.
    uint64_t getSize() { return this->size; }
    /// Destructor.
    ~DataNode() { }
};
//---------------------------------------------------------------------------
class AbstractOperatorNode : public AbstractNode {
    public:
    /// A list of columns that are passed to the next `DataNode`
    /// after the application of the operator.
    std::vector<SelectInfo> selectionsInfo;

    /// Frees any resources allocated by the node.
    void freeResources() { this->selectionsInfo.clear(); }

    /// Pushes to `pairs`, pairs of the form `{rowIndex, rowValue}`
    /// for the values in `values`.
    static void getValuesIndexed(IteratorPair &values,
                                 std::vector<uint64Pair> &pairs);

    /// Pushes from `inNode.dataValues` to `outNodes.dataValues` the
    /// values specified by `indices` for the specified columns
    /// in `selections`.
    static void pushSelections(std::vector<SelectInfo> &selections,
                               std::vector<uint64_t> &indices,
                               AbstractDataNode *inNode,
                               DataNode *outNode);

    /// Pushes the values specifies by `indices` of the `valIter`
    /// iterator to `outValues`.
    static void pushValuesByIndex(IteratorPair &valIter,
                                  std::vector<uint64_t> &indices,
                                  std::vector<uint64_t> &outValues);
};
//---------------------------------------------------------------------------
class JoinOperatorNode : public AbstractOperatorNode {
    public:
    /// Reference to the corresponding `PredicateInfo` instance.
    PredicateInfo &info;

    /// Joins the two input `DataNode` instances.
    void execute();

    JoinOperatorNode(struct PredicateInfo &info) : info(info) {}

    /// Performs merge join between `leftPairs` and `rightPairs`.
    static void mergeJoin(std::vector<uint64Pair> &leftPairs,
                          std::vector<uint64Pair> &rightPairs,
                          std::pair<std::vector<uint64_t>, std::vector<uint64_t>> &indexPairs);

    /// Pushes to `pairs`, pairs of the form `{rowIndex, rowValue}`
    /// sorted by value.
    static void getValuesIndexedSorted(std::vector<uint64Pair> &pairs,
                                       SelectInfo &selection,
                                       AbstractDataNode* inNode);
    ~JoinOperatorNode() { }
};
//---------------------------------------------------------------------------
class FilterOperatorNode : public AbstractOperatorNode {
    public:
    /// Reference to the corresponding `FilterInfo` instance.
    FilterInfo& info;

    /// Filters the input `DataNode` instance.
    void execute();

    FilterOperatorNode(FilterInfo &info) : info(info) {}

    ~FilterOperatorNode() { }
};
//---------------------------------------------------------------------------
class AggregateOperatorNode : public AbstractOperatorNode {
    public:
    /// Calculates sums for the columns in the `selectionsInfo` vector.
    void execute();

    ~AggregateOperatorNode() { }
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
