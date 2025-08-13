#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CMergeJoinToBOSSMergeJoin : public Translator {
 public:
  CMergeJoinToBOSSMergeJoin() = default;
  ~CMergeJoinToBOSSMergeJoin() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for MergeJoin
  static RetTypeC2B<ColSet> GetJoinCondExpr(const CExpression* expr,
                             CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
};

}  // namespace cexpressiontoboss::translation
