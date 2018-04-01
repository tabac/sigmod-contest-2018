#pragma once
#include <vector>
#include <variant>
#include <cstdint>
#include "Plan.hpp"
#include "Mixins.hpp"
//---------------------------------------------------------------------------
class Executor {
    private:
    /// A flag used in combination with `syncPair` below.
    static bool syncFlag;
    /// SyncPair for signaling the Executor that an operator
    /// has finished processing.
    static SyncPair syncPair;
    /// Static vector of the threads used in operator execution.
    static std::vector<std::thread> threads;

    public:
    /// Executes the given `Plan`, stores results in `resultsInfo`.
    static void executePlan(Plan &plan, std::vector<ResultInfo> &resultsInfo);
    /// Executes the operator of an `OperatorNode`.
    static void executeOperator(AbstractNode *node);
    /// Sets the `syncFlag` to `true`.
    static void setFlag(void) { Executor::syncFlag = true; }
    /// Sets the `syncFlag` to `false`.
    static void unsetFlag(void) { Executor::syncFlag = false; }
    /// Called by `OperatorNode` threads, notifies the `Executor`
    /// thread that an `OperatorNode` thread has finished processing.
    static void notify(void);
    /// Called by the `Executor` thread, waits for a notification
    /// by the `OperatorNode` threads.
    static void wait(void);
};
//---------------------------------------------------------------------------

