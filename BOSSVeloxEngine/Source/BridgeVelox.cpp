// Data structures and functions in this file are referenced/copied from Velox prototype for Arrow
// Velox conversion.

#include "BridgeVelox.h"

#include <utility>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

#define VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_64B_MAX(TEMPLATE_FUNC, typeKind, ...)                   \
  [&]() {                                                                                          \
    switch(typeKind) {                                                                             \
    case ::facebook::velox::TypeKind::BOOLEAN: {                                                   \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::BOOLEAN>(__VA_ARGS__);                     \
    }                                                                                              \
    case ::facebook::velox::TypeKind::INTEGER: {                                                   \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::INTEGER>(__VA_ARGS__);                     \
    }                                                                                              \
    case ::facebook::velox::TypeKind::TINYINT: {                                                   \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::TINYINT>(__VA_ARGS__);                     \
    }                                                                                              \
    case ::facebook::velox::TypeKind::SMALLINT: {                                                  \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::SMALLINT>(__VA_ARGS__);                    \
    }                                                                                              \
    case ::facebook::velox::TypeKind::BIGINT: {                                                    \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::BIGINT>(__VA_ARGS__);                      \
    }                                                                                              \
    case ::facebook::velox::TypeKind::REAL: {                                                      \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::REAL>(__VA_ARGS__);                        \
    }                                                                                              \
    case ::facebook::velox::TypeKind::DOUBLE: {                                                    \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::DOUBLE>(__VA_ARGS__);                      \
    }                                                                                              \
    default:                                                                                       \
      VELOX_FAIL("not a scalar type! kind: {}", mapTypeKindToName(typeKind));                      \
    }                                                                                              \
  }()

namespace boss::engines::velox {

// Optionally, holds shared_ptrs pointing to the BossArray object that
// holds the buffer object that describes the BossArray,
// which will be released to signal that we will no longer hold on to the data
// and the shared_ptr deleters should run the release procedures if no one
// else is referencing the objects.
struct BufferViewReleaser {

  explicit BufferViewReleaser(std::shared_ptr<BossArray>&& bossArray)
      : arrayReleaser_(std::move(bossArray)) {}

  void addRef() const {}

  void release() const {}

private:
  std::shared_ptr<BossArray> arrayReleaser_;
};

// Wraps a naked pointer using a Velox buffer view, without copying it. This
// buffer view uses shared_ptr to manage reference counting and releasing for
// the BossArray object
BufferPtr wrapInBufferViewAsOwner(const void* buffer, size_t length,
                                  std::shared_ptr<BossArray>&& arrayReleaser) {
  return BufferView<BufferViewReleaser>::create(static_cast<const uint8_t*>(buffer), length,
                                                {BufferViewReleaser(std::move(arrayReleaser))});
}

// Dispatch based on the type.
template <TypeKind kind>
VectorPtr createFlatVector(memory::MemoryPool* pool, TypePtr const& type, BufferPtr nulls,
                           size_t length, BufferPtr values) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<FlatVector<T>>(pool, type, nulls, length, values,
                                         std::vector<BufferPtr>(), SimpleVectorStats<T>{},
                                         std::nullopt, std::nullopt);
}

TypePtr importFromBossType(BossType bossType) {
  switch(bossType) {
  case boss::engines::velox::bINTEGER:
    return INTEGER();
  case boss::engines::velox::bBIGINT:
    return BIGINT();
  case boss::engines::velox::bREAL:
    return REAL();
  case boss::engines::velox::bDOUBLE:
    return DOUBLE();
  }
  VELOX_USER_FAIL("Unable to convert '{}' BossType format type to Velox.", bossType)
}

VectorPtr importFromBossAsOwner(BossType bossType, BossArray&& bossArray,
                                memory::MemoryPool* pool) {
  VELOX_CHECK_GE(bossArray.length, 0, "Array length needs to be non-negative.")

  // First parse and generate a Velox type.
  auto type = importFromBossType(bossType);

  // Wrap the nulls buffer into a Velox BufferView (zero-copy). Null buffer size
  // needs to be at least one bit per element.
  BufferPtr nulls = nullptr;

  // Other primitive types.
  VELOX_CHECK(type->isPrimitiveType(), "Conversion of '{}' from Boss not supported yet.",
              type->toString())

  // Wrap the values buffer into a Velox BufferView - zero-copy.
  const auto* buffer = bossArray.buffers;
  auto length = bossArray.length * type->cppSizeInBytes();
  auto arrayReleaser = std::make_shared<BossArray>(std::move(bossArray));
  auto values = wrapInBufferViewAsOwner(buffer, length, std::move(arrayReleaser));

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_64B_MAX(createFlatVector, type->kind(), pool, type,
                                                    nulls, bossArray.length, values);
}

BufferPtr importFromBossAsOwnerBuffer(BossArray&& bossArray) {
  VELOX_CHECK_GE(bossArray.length, 0, "Array length needs to be non-negative.")

  // Wrap the values buffer into a Velox BufferView - zero-copy.
  const auto* buffer = bossArray.buffers;
  auto length = bossArray.length * sizeof(int32_t); // assuming always int32 type!
  auto arrayReleaser = std::make_shared<BossArray>(std::move(bossArray));
  return wrapInBufferViewAsOwner(buffer, length, std::move(arrayReleaser));
}

std::vector<RowVectorPtr> myReadCursor(CursorParameters const& params,
                                       std::unique_ptr<TaskCursor>& cursor,
                                       std::function<void(exec::Task*)> addSplits) {
  cursor = TaskCursor::create(params);
  // 'result' borrows memory from cursor so the life cycle must be shorter.
  std::vector<RowVectorPtr> result;
  auto* task = cursor->task().get();
  addSplits(task);

  while(cursor->moveNext()) {
    // std::cout << "result for id:" << std::this_thread::get_id() << std::endl;
    result.push_back(cursor->current());
    addSplits(task);
  }

  return std::move(result);
}

std::vector<RowVectorPtr>
veloxRunQueryParallel(CursorParameters const& params, std::unique_ptr<TaskCursor>& cursor,
                      std::vector<std::pair<core::PlanNodeId, size_t>> const& scanIds) {
  try {
    bool noMoreSplits = false;
    auto addSplits = [&](exec::Task* task) {
      if(!noMoreSplits) {
        for(auto const& [scanId, numSpans] : scanIds) {
          for(size_t i = 0; i < numSpans; ++i) {
            task->addSplit(scanId, exec::Split(std::make_shared<BossConnectorSplit>(
                                       kBossConnectorId, numSpans, i)));
          }
          task->noMoreSplits(scanId);
        }
      }
      noMoreSplits = true;
    };
    auto result = myReadCursor(params, cursor, addSplits);
    return result;
  } catch(std::exception const& e) {
    LOG(ERROR) << "Query terminated with: " << e.what();
    return {};
  }
}

RowVectorPtr makeRowVectorNoCopy(std::shared_ptr<RowType const>& schema,
                                 std::vector<VectorPtr>&& children, memory::MemoryPool* pool) {
  size_t const vectorSize = children.empty() ? 0 : children.front()->size();
  return std::make_shared<RowVector>(pool, schema, BufferPtr(nullptr), vectorSize,
                                     std::move(children));
}

} // namespace boss::engines::velox

auto fmt::formatter<boss::engines::velox::BossType>::format(boss::engines::velox::BossType type,
                                                            format_context& ctx) const {
  string_view name = "unknown";
  switch(type) {
  case boss::engines::velox::bINTEGER:
    name = "bInteger";
    break;
  case boss::engines::velox::bBIGINT:
    name = "bBIGINT";
    break;
  case boss::engines::velox::bREAL:
    name = "bREAL";
    break;
  case boss::engines::velox::bDOUBLE:
    name = "bDOUBLE";
    break;
  }
  return formatter<string_view>::format(name, ctx);
}
