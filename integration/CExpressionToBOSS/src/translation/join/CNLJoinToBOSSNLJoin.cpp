#include "translation/join/CNLJoinToBOSSNLJoin.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CNLJoinToBOSSNLJoin::Match(const CExpression* expr) {
  if (!expr) return false;

  COperator::EOperatorId opid = expr->Pop()->Eopid();

  // Match all NL join types
  return (opid == COperator::EopPhysicalInnerNLJoin ||
          opid == COperator::EopPhysicalLeftOuterNLJoin ||
          opid == COperator::EopPhysicalLeftSemiNLJoin ||
          opid == COperator::EopPhysicalLeftAntiSemiNLJoin ||
          opid == COperator::EopPhysicalLeftAntiSemiNLJoinNotIn ||
          opid == COperator::EopPhysicalInnerIndexNLJoin ||
          opid == COperator::EopPhysicalLeftOuterIndexNLJoin);
}

RetTypeC2B<ColSet> CNLJoinToBOSSNLJoin::GetJoinCondExpr(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  CExpression* pexprJoinCond = (*expr)[2];
  return converter.ConvertScalar(pexprJoinCond, {});
}

Expression CNLJoinToBOSSNLJoin::GetUseIndexExpr(COperator::EOperatorId opid) {
  // Get the operator ID to determine the join type
  bool isIndexJoin = (opid == COperator::EopPhysicalInnerIndexNLJoin ||
                      opid == COperator::EopPhysicalLeftOuterIndexNLJoin);
  return "UseIndex"_(isIndexJoin);
}

}  // namespace cexpressiontoboss::translation
