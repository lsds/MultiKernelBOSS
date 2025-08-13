#include "translation/limit/CPhysicalLimitToBOSSPhysicalLimit.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CPhysicalLimitToBOSSPhysicalLimit::Match(const CExpression* expr) {
  return (expr->Pop()->Eopid() == COperator::EopPhysicalLimit);
}

RetTypeC2B<ColSet> CPhysicalLimitToBOSSPhysicalLimit::GetOffsetExpr(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  ColSet requiredColumns;
  bool success = true;
  CExpression* pexprOffset = (*expr)[1];
  RetTypeC2B<ColSet> offsetExpr = converter.ConvertScalar(pexprOffset, {});
  success &= offsetExpr.success;
  requiredColumns.insert(offsetExpr.aux.begin(), offsetExpr.aux.end());
  return {"Offset"_(std::move(offsetExpr.expr)), success, requiredColumns};

}

RetTypeC2B<ColSet> CPhysicalLimitToBOSSPhysicalLimit::GetLimitCountExpr(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  ColSet requiredColumns;
  bool success = true;
  CExpression* pexprLimitCount = (*expr)[2];
  RetTypeC2B<ColSet> limitCountExpr = converter.ConvertScalar(pexprLimitCount, {});
  success &= limitCountExpr.success;
  requiredColumns.insert(limitCountExpr.aux.begin(), limitCountExpr.aux.end());
  return {"Count"_(std::move(limitCountExpr.expr)), success, requiredColumns};

}
}  // namespace cexpressiontoboss::translation