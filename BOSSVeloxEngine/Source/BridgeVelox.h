#pragma once

#include "BossConnector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace boss::engines::velox {
std::string const kBossConnectorId = "boss";
using ExpressionSpanArgument = boss::expressions::ExpressionSpanArgument;

enum BossType { bINTEGER = 0, bBIGINT, bREAL, bDOUBLE };

struct BossArray {
  BossArray(int64_t span_size, void const* span_begin, ExpressionSpanArgument&& span)
      : length(span_size), buffers(span_begin), holdSpan(std::move(span)) {}

  BossArray(BossArray&& other) noexcept
      : length(other.length), buffers(other.buffers), holdSpan(std::move(other.holdSpan)) {}

  BossArray& operator=(BossArray&& other) {
    length = other.length;
    buffers = other.buffers;
    holdSpan = std::move(other.holdSpan);
    return *this;
  }

  BossArray(BossArray const& bossArray) = delete;
  BossArray& operator=(BossArray const& other) = delete;

  ~BossArray() = default;

  ExpressionSpanArgument holdSpan;
  int64_t length;
  void const* buffers;
};

VectorPtr importFromBossAsOwner(BossType bossType, BossArray&& bossArray, memory::MemoryPool* pool);

BufferPtr importFromBossAsOwnerBuffer(BossArray&& bossArray);

std::vector<RowVectorPtr>
veloxRunQueryParallel(CursorParameters const& params, std::unique_ptr<TaskCursor>& cursor,
                      std::vector<std::pair<core::PlanNodeId, size_t>> const& scanIds);

void veloxPrintResults(std::vector<RowVectorPtr> const& results);

RowVectorPtr makeRowVectorNoCopy(std::shared_ptr<RowType const>& schema,
                                 std::vector<VectorPtr>&& children, memory::MemoryPool* pool);

} // namespace boss::engines::velox

template <> struct fmt::formatter<boss::engines::velox::BossType> : formatter<string_view> {
  auto format(boss::engines::velox::BossType c, format_context& ctx) const;
};
