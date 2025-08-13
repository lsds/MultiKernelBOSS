#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CSortToBOSSSort : public Translator {
 public:
  CSortToBOSSSort() = default;
  ~CSortToBOSSSort() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for Sort
  static std::pair<Expression, std::unordered_set<std::string>> GetSortKeyList(const CExpression* expr);
};

}  // namespace cexpressiontoboss::translation
