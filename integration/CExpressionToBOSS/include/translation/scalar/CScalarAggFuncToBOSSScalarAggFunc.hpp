#pragma once

#include "translation/ScalarTranslator.hpp"

namespace cexpressiontoboss::translation {

class CScalarAggFuncToBOSSScalarAggFunc : public ScalarTranslator {
 public:
  CScalarAggFuncToBOSSScalarAggFunc() = default;
  ~CScalarAggFuncToBOSSScalarAggFunc() override = default;
  bool Match(const CExpression* expr) override;
  RetTypeC2B<ColSet> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       EmptyStruct const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation