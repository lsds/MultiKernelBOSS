#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CNLJoinToBOSSNLJoin : public Translator {
 public:
  CNLJoinToBOSSNLJoin() = default;
  ~CNLJoinToBOSSNLJoin() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for NLJoin
  static RetTypeC2B<ColSet> GetJoinCondExpr(const CExpression* expr,
                             CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
  static Expression GetUseIndexExpr(COperator::EOperatorId opid);
};

}  // namespace cexpressiontoboss::translation
