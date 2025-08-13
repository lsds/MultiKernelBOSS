#include "translation/join/CMergeJoinToBOSSMergeJoin.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CMergeJoinToBOSSMergeJoin::Match(const CExpression* expr) {
  return (expr->Pop()->Eopid() == COperator::EopPhysicalFullMergeJoin);
}

RetTypeC2B<ColSet> CMergeJoinToBOSSMergeJoin::GetJoinCondExpr(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  CExpression* pexprJoinCond = (*expr)[2];
  return converter.ConvertScalar(pexprJoinCond, {});
}
}  // namespace cexpressiontoboss::translation
