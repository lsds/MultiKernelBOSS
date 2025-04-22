#pragma once

#include "velox/connectors/Connector.h"

using namespace facebook::velox;

namespace boss::engines::velox {

struct BossConnectorSplit : public connector::ConnectorSplit {
  explicit BossConnectorSplit(std::string const& connectorId, size_t totalParts = 1,
                              size_t partNumber = 0)
      : ConnectorSplit(connectorId), totalParts(totalParts), partNumber(partNumber) {
    VELOX_CHECK_GE(totalParts, 1, "totalParts must be >= 1");
    VELOX_CHECK_GT(totalParts, partNumber, "totalParts must be > partNumber");
  }

  // In how many parts the generated TPC-H table will be segmented, roughly
  // `rowCount / totalParts`
  size_t totalParts{1};

  // Which of these parts will be read by this split.
  size_t partNumber{0};
};

} // namespace boss::engines::velox
