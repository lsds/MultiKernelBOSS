#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalHashAggToBOSSHashAgg : public Translator {
 public:
  CPhysicalHashAggToBOSSHashAgg() = default;
  ~CPhysicalHashAggToBOSSHashAgg() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for HashAgg
  static RetTypeC2B<ColSet> GetProjectList(const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);  
  static std::pair<Expression, ColSet> GetGroupingColumns(const CExpression* expr);
  static Expression GetIsDeduplicate(const CExpression* expr);
};

}  // namespace cexpressiontoboss::translation
