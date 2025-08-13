#include "translation/agg/CPhysicalScalarAggToBOSSScalarAgg.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
bool CPhysicalScalarAggToBOSSScalarAgg::Match(const CExpression* expr) {
  return (expr->Pop()->Eopid() == COperator::EopPhysicalScalarAgg);
}

RetTypeC2B<ColSet> CPhysicalScalarAggToBOSSScalarAgg::GetProjectList(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  CExpression* pexprProjList = (*expr)[1];
  return converter.ConvertScalar(pexprProjList, {});
}

}  // namespace cexpressiontoboss::translation