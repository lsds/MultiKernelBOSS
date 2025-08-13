#include "translation/project/CPhysicalComputeScalarToBOSSProject.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CPhysicalComputeScalarToBOSSProject::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopPhysicalComputeScalar;
}

RetTypeC2B<ColSet> CPhysicalComputeScalarToBOSSProject::GetProjectListExpr(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  CExpression* projectListExpr = (*expr)[1];
  return converter.ConvertScalar(projectListExpr, {});
}
}  // namespace cexpressiontoboss::translation
