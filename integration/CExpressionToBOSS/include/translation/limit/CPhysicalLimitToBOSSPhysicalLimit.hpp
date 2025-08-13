#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalLimitToBOSSPhysicalLimit : public Translator {
 public:
  CPhysicalLimitToBOSSPhysicalLimit() = default;
  ~CPhysicalLimitToBOSSPhysicalLimit() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for Limit
  static RetTypeC2B<ColSet> GetOffsetExpr(const CExpression* expr,
                                 CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
  static RetTypeC2B<ColSet> GetLimitCountExpr(const CExpression* expr,
                               CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
};

}  // namespace cexpressiontoboss::translation
