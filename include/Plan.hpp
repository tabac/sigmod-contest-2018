#pragma once
#include <future>
#include <vector>
#include <cstdint>
#include <cassert>
#include <string>
#include <experimental/optional>
#include "Mixins.hpp"
#include "Parser.hpp"
#include "Index.hpp"
//---------------------------------------------------------------------------
enum NodeStatus { fresh, processing, processed };
//---------------------------------------------------------------------------
class ResultInfo {
    public:
    /// Query results.
    std::vector<optional<uint64_t>> results;

    ResultInfo(std::vector<uint64_t> results, unsigned size);
    /// Prints the `results` vector to stdout.
    void printResultInfo() const;
    /// Prints the `ResultInfo` vector to stdout.
    static void printResults(const std::vector<ResultInfo> resultsInfo);
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
    virtual void execute(std::vector<std::thread> &threads) = 0;

    /// Status setter.
    void setStatus(NodeStatus status) { this->status = status; };

    /// Status getters.
    bool isStatusFresh() { return status == NodeStatus::fresh; };
    bool isStatusProcessing() { return status == NodeStatus::processing; };
    bool isStatusProcessed() { return status == NodeStatus::processed; };

    /// Resets the nodes status, adjacency lists.
    /// Used for the relations that are reused between
    /// query batches.
    void resetStatus();

    /// Another total fail.
    virtual bool isBaseRelation() const = 0;

    /// Connects the `left` with the `right` nodes, `left` precedes `right`.
    static void connectNodes(AbstractNode *left, AbstractNode *right);

    /// Frees any resources allocated by the node.
    virtual void freeResources() = 0;

    /// Constructor.
    AbstractNode() : status(NodeStatus::fresh), visited(0) { }
    /// Virtual destructor.
    virtual ~AbstractNode() { }

#ifndef NDEBUG
    std::string label;
#endif

};
//---------------------------------------------------------------------------
class AbstractDataNode : public AbstractNode, public DataReaderMixin {
    public:
    /// A vector of `SelectInfo` instances of the columns
    /// appearing in the `data` vector.
    std::vector<SelectInfo> columnsInfo;

    /// Should return the size, that is the number of tuples.
    virtual uint64_t getSize() const = 0;

    bool isBaseRelation() const { return false; }

    virtual SortedIndex *getIndex(const SelectInfo &) = 0;
};
//---------------------------------------------------------------------------
class DataNode : public AbstractDataNode {
    public:
    /// The number of tuples (rows).
    uint64_t size;
    /// A table in columnar format with "value" entries.
    std::vector<uint64_t> dataValues;

    /// Checks if the nodes it depends on are `processed`
    /// and if so sets its flag to processed too.
    void execute(std::vector<std::thread> &);
    /// Frees any resources allocated by the node.
    void freeResources() { this->dataValues.clear(); this->columnsInfo.clear(); }

    /// Returns `nullopt` for a `DataNode`. The ids are the indices
    /// in the case of a column.
    optional<IteratorPair> getIdsIterator(const SelectInfo&,
                                          const FilterInfo* filterInfo);
    /// Returns an `IteratorPair` over all the `DataNode`'s values
    /// of the column specified by `selectInfo`.
    /// Ignores `filterInfo`, requires it being `NULL`.
    optional<IteratorPair> getValuesIterator(const SelectInfo& selectInfo,
                                             const FilterInfo* filterInfo);

    /// Returns the size, that is the number of tuples.
    uint64_t getSize() const { return this->size; }

    SortedIndex *getIndex(const SelectInfo &) { return NULL; }

    /// Empty constructor.
    DataNode() { }
    /// Disable copy constructor.
    DataNode(const DataNode&)=delete;
    /// Destructor.
    ~DataNode() { }
};
//---------------------------------------------------------------------------
class AbstractOperatorNode : public AbstractNode {
    public:
    const unsigned queryId;

    std::vector<SelectInfo> selections;

    /// Frees any resources allocated by the node.
    void freeResources() { this->selections.clear(); }

    /// Pushes from `inNode.dataValues` to `outNodes.dataValues` the
    /// values specified by `indices` for the specified columns
    /// in `selections`.
    template <size_t I, typename T>
    static void pushSelections(const std::vector<SelectInfo> &selections,
                               const T &indices,
                               AbstractDataNode *inNode,
                               DataNode *outNode);

    /// Pushes the values specifies by `indices` of the `valIter`
    /// iterator to `outValues`.
    template <size_t I, typename T>
    static void pushValuesByIndex(const IteratorPair &valIter,
                                  const T &indices,
                                  DataNode *outNode);

    virtual bool hasBinding(const unsigned binding) const = 0;

    virtual bool hasSelection(const SelectInfo &selection) const = 0;

    bool isBaseRelation() const { return false; }

    AbstractOperatorNode(const unsigned queryId) : queryId(queryId) { }

    ~AbstractOperatorNode() { }
};
//---------------------------------------------------------------------------
class JoinOperatorNode : public AbstractOperatorNode {
    public:
    /// Reference to the corresponding `PredicateInfo` instance.
    PredicateInfo &info;

    /// Joins the two input `DataNode` instances.
    void execute(std::vector<std::thread> &threads);

    void executeAsync(void);

    /// Performs sort-merge join between `leftPairs` and `rightPairs`.
    template <typename T>
    static void mergeJoinSeq(const SelectInfo &left, const SelectInfo &right,
                             AbstractDataNode *leftNode, AbstractDataNode *rightNode,
                             T &indexPairs);

    /// Performs sort-merge join between `leftPairs` and `rightPairs`.
    /// Does so in parallel using a `tbb::concurrent_vector`.
    template <typename T>
    static void mergeJoinPar(const SelectInfo &left, const SelectInfo &right,
                             AbstractDataNode *leftNode, AbstractDataNode *rightNode,
                             T &indexPairs);

    /// Performs hash join between `leftPairs` and `rightPairs`.
    template <typename T>
    static void hashJoinSeq(const SelectInfo &left, const SelectInfo &right,
                            AbstractDataNode *leftNode, AbstractDataNode *rightNode,
                            T &indexPairs);

    /// Performs hash join between `leftPairs` and `rightPairs` (in parallel).
    template <typename T>
    static void hashJoinPar(const SelectInfo &left, const SelectInfo &right,
                            AbstractDataNode *leftNode, AbstractDataNode *rightNode,
                            T &indexPairs);

    static void hashJoinBuildPar(const std::vector<uint64Pair> &pairs,
                                 uint64VecMapCc &map);

    template <bool B, typename T>
    static void hashJoinProbePar(const std::vector<uint64Pair> &pairs,
                                 const uint64VecMapCc &map,
                                 T &indexPairs);

    /// Returns a tuple with a boolean and pairs of the form
    /// `{rowIndex, rowValue}` sorted by value. The boolean
    /// indicates whether the memory has to be freed or not.
    static std::pair<bool, std::vector<uint64Pair>*> getValuesIndexedSorted(
        const SelectInfo &selection, AbstractDataNode* inNode);

    /// Populates `pairs` with pairs of the form `{rowIndex, rowValue}`.
    static void getValuesIndexed(const SelectInfo &selection,
                                 AbstractDataNode *inNode,
                                 std::vector<uint64Pair> &pairs);

    /// Updates `this->selections` sets the predicate's
    /// selections to `sorted == true`.
    void updateSelectionsSorted(void);

    static bool isSelectionSorted(const SelectInfo &selection,
                                  const AbstractDataNode *inNode);

    bool hasBinding(const unsigned binding) const {
        return this->info.left.binding == binding || this->info.right.binding == binding;
    }

    bool hasSelection(const SelectInfo &selection) const {
        return this->info.left == selection || this->info.right == selection;
    }

    /// Constructor.
    JoinOperatorNode(const unsigned queryId, PredicateInfo &info) :
        AbstractOperatorNode(queryId), info(info) { }
    /// Disable copy constructor.
    JoinOperatorNode(const JoinOperatorNode&)=delete;
    /// Destructor.
    ~JoinOperatorNode() { }
};
//---------------------------------------------------------------------------
class FilterOperatorNode : public AbstractOperatorNode {
    public:
    /// Reference to the corresponding `FilterInfo` instance.
    FilterInfo& info;

    /// Filters the input `DataNode` instance.
    void execute(std::vector<std::thread> &threads);

    void executeAsync(void);

    bool hasBinding(const unsigned binding) const {
        return this->info.filterColumn.binding == binding;
    }

    bool hasSelection(const SelectInfo &selection) const {
        return this->info.filterColumn == selection;
    }

    /// Constructor.
    FilterOperatorNode(const unsigned queryId, FilterInfo &info) :
        AbstractOperatorNode(queryId), info(info) { }
    /// Disable copy constructor.
    FilterOperatorNode(const FilterOperatorNode&)=delete;
    /// Destructor.
    ~FilterOperatorNode() { }
};
//---------------------------------------------------------------------------
class FilterJoinOperatorNode : public AbstractOperatorNode {
    public:
    /// Reference to the corresponding `PredicateInfo` instance.
    PredicateInfo &info;

    /// Filters the input `DataNode` instance.
    void execute(std::vector<std::thread> &threads);

    void executeAsync(void);

    bool hasBinding(const unsigned binding) const {
        return this->info.left.binding == binding;
    }

    bool hasSelection(const SelectInfo &selection) const {
        return this->info.left == selection;
    }

    /// Constructor.
    FilterJoinOperatorNode(const unsigned queryId, PredicateInfo &info) :
        AbstractOperatorNode(queryId), info(info) { }
    /// Disable copy constructor.
    FilterJoinOperatorNode(const FilterJoinOperatorNode&)=delete;
    /// Destructor.
    ~FilterJoinOperatorNode() { }
};
//---------------------------------------------------------------------------
class AggregateOperatorNode : public AbstractOperatorNode {
    public:
    /// Calculates sums for the columns in the `selections` vector.
    void execute(std::vector<std::thread> &threads);

    void executeAsync(void);

    bool hasBinding(const unsigned) const { return true; }

    bool hasSelection(const SelectInfo &) const { return true; }

    /// Constructor.
    AggregateOperatorNode(const unsigned queryId) :
        AbstractOperatorNode(queryId) { }
    /// Disable copy constructor.
    AggregateOperatorNode(const AggregateOperatorNode&)=delete;
    /// Destructor
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
