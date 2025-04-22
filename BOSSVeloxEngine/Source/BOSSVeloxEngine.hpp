#pragma once

#include "BridgeVelox.h"

#include <Expression.hpp>

#include <velox/common/memory/MemoryPool.h>

#include <memory>
#include <thread>
#include <unordered_map>

#ifdef _WIN32
extern "C" {
__declspec(dllexport) BOSSExpression* evaluate(BOSSExpression* e);
__declspec(dllexport) void reset();
}
#endif // _WIN32

// #define USE_NEW_TABLE_FORMAT

// #define TAKE_OWNERSHIP_OF_TASK_POOLS // requires velox patch to Task.h

// #define DebugInfo

namespace boss::engines::velox {

class Engine {

public:
  Engine(Engine&) = delete;

  Engine& operator=(Engine&) = delete;

  Engine(Engine&&) = default;

  Engine& operator=(Engine&&) = delete;

  Engine();

  ~Engine();

  boss::Expression evaluate(boss::Expression&& e);
  boss::Expression evaluate(boss::ComplexExpression&& e);

private:
  std::unordered_map<std::thread::id, std::shared_ptr<facebook::velox::memory::MemoryPool>>
      threadPools_;

  int32_t maxThreads = 1;
  int32_t internalBatchNumRows = 0;
  int32_t minimumOutputBatchNumRows = 0;
  bool hashAdaptivityEnabled = true;

  PlanBuilder buildOperatorPipeline(ComplexExpression&& e,
                                    std::vector<std::pair<core::PlanNodeId, size_t>>& scanIds,
                                    memory::MemoryPool& pool,
                                    std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
                                    int& tableCnt, int& joinCnt);
};

} // namespace boss::engines::velox
