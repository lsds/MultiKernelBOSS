#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CFilterToBOSSFilter : public Translator {
 public:
  CFilterToBOSSFilter() = default;
  virtual ~CFilterToBOSSFilter() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for Filter
  static RetTypeC2B<ColSet> GetPredicateExpr(const CExpression* expr,
                              CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
};

}  // namespace cexpressiontoboss::translation
