#include "BossConnector.h"

// #ifdef DebugInfo
#include <iostream>
// #endif // DebugInfo

namespace boss::engines::velox {

std::string BossTableHandle::toString() const {
  return fmt::format("table: {}, spanRowCount size: {}", tableName_, spanRowCountVec_.size());
}

BossDataSource::BossDataSource(
    std::shared_ptr<RowType const> const& outputType,
    std::shared_ptr<connector::ConnectorTableHandle> const& tableHandle,
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> const& columnHandles,
    memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool), outputType_(outputType),
      bossTableHandle_(std::dynamic_pointer_cast<BossTableHandle>(tableHandle)),
      bossTableName_(bossTableHandle_->getTable()),
      bossRowDataVec_(bossTableHandle_->getRowDataVec()),
      bossSpanRowCountVec_(bossTableHandle_->getSpanRowCountVec()) {
  VELOX_CHECK_NOT_NULL(bossTableHandle_, "TableHandle must be an instance of BossTableHandle");

  auto const& bossTableSchema = bossTableHandle_->getTableSchema();
  VELOX_CHECK_NOT_NULL(bossTableSchema, "BossSchema can't be null.");

  outputColumnMappings_.reserve(outputType->size());

  for(auto const& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(it != columnHandles.end(),
                "ColumnHandle is missing for output column '{}' on table '{}'", outputName,
                bossTableName_);

    auto handle = std::dynamic_pointer_cast<BossColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(handle,
                         "ColumnHandle must be an instance of BossColumnHandle "
                         "for '{}' on table '{}'",
                         handle->name(), bossTableName_);

    auto idx = bossTableSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(idx != std::nullopt, "Column '{}' not found on TPC-H table '{}'.", handle->name(),
                bossTableName_);
    outputColumnMappings_.emplace_back(*idx);
  }
}

void BossDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(currentSplit_,
                   "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<BossConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(currentSplit_, "Wrong type of split for BossDataSource.");

  spanCountIdx_ = currentSplit_->partNumber;
  splitOffset_ = 0;
  splitEnd_ = bossSpanRowCountVec_.at(spanCountIdx_) - splitOffset_;

#ifdef DebugInfo
  std::cout << "addSplit for table " << bossTableName_ << std::endl;
  std::cout << "    bossSpanRowCountVec_.size(): " << bossSpanRowCountVec_.size() << std::endl;
  std::cout << "    spanCountIdx_: " << spanCountIdx_ << std::endl;
  std::cout << "    splitOffset_: " << splitOffset_ << std::endl;
  std::cout << "    splitEnd_: " << splitEnd_ << std::endl;
#endif // DebugInfo
}

RowVectorPtr BossDataSource::getBossData(uint64_t length) {
#ifdef DebugInfo
  std::cout << "getBossData: spanCountIdx_=" << spanCountIdx_ << " splitOffset_=" << splitOffset_
            << " length=" << length << std::endl;
#endif
  assert(splitOffset_ <= INT_MAX);
  assert(length <= INT_MAX);

  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());
  for(auto channelIdx : outputColumnMappings_) {
    children.emplace_back(
        bossRowDataVec_.at(spanCountIdx_)->childAt(channelIdx)->slice(splitOffset_, length));
  }

  return std::make_shared<RowVector>(pool_, outputType_, BufferPtr(nullptr), length,
                                     std::move(children));
}

std::optional<RowVectorPtr> BossDataSource::next(uint64_t size, ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(currentSplit_, "No split to process. Call addSplit() first.");

  auto maxRows = std::min(size, (splitEnd_ - splitOffset_));
  auto outputVector = getBossData(maxRows);

  // If the split is exhausted.
  if(!outputVector || outputVector->size() == 0) {
    currentSplit_ = nullptr;
    return nullptr;
  }

  splitOffset_ += maxRows;
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  return std::move(outputVector);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<BossConnectorFactory>())

} // namespace boss::engines::velox
