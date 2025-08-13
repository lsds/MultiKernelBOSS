#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CHashJoinToBOSSHashJoin : public Translator {
 public:
  CHashJoinToBOSSHashJoin() = default;
  ~CHashJoinToBOSSHashJoin() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for HashJoin
  static std::pair<std::pair<Expression, Expression>, std::pair<bool, ColSet>> GetInnerAndOuterKeyLists(
      const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
  static RetTypeC2B<ColSet> GetJoinCondExpr(const CExpression* expr,
                             CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
};

}  // namespace cexpressiontoboss::translation
