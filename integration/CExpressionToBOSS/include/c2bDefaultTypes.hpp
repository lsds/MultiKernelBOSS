#pragma once

#include "TranslatorBase.hpp"

namespace cexpressiontoboss {
namespace translation {

struct EmptyStruct {};

using ColSet = std::unordered_set<std::string>;

struct ProjectInfo {
  std::unordered_set<std::string> requiredColumns;
  COperator::EOperatorId parentOp;
};

}
}