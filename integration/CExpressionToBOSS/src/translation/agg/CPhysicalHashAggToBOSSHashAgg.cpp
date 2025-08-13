#include "translation/agg/CPhysicalHashAggToBOSSHashAgg.hpp"

#include "CExpressionToBOSS.hpp"

// NOTE COUNT(*) is just represent be Count. (without any args)

namespace cexpressiontoboss::translation {
bool CPhysicalHashAggToBOSSHashAgg::Match(const CExpression* expr) {
  COperator::EOperatorId opid = expr->Pop()->Eopid();
  return (opid == COperator::EopPhysicalHashAgg ||
          opid == COperator::EopPhysicalHashAggDeduplicate);
}

RetTypeC2B<ColSet> CPhysicalHashAggToBOSSHashAgg::GetProjectList(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  CExpression* pexprProjList = (*expr)[1];
  return converter.ConvertScalar(pexprProjList, {});
}

std::pair<Expression, ColSet> CPhysicalHashAggToBOSSHashAgg::GetGroupingColumns(
    const CExpression* expr) {
  ColSet requiredColumns;
  // Get the grouping columns
  CPhysicalAgg* pPhysicalAgg = dynamic_cast<CPhysicalAgg*>(expr->Pop());
  if (!pPhysicalAgg) {
    throw std::runtime_error("Failed to cast to CPhysicalAgg");
  }

  // Extract grouping columns
  const CColRefArray* pdrgpcrGroupingCols = pPhysicalAgg->PdrgpcrGroupingCols();
  ExpressionArguments groupingColsArgs;

  if (pdrgpcrGroupingCols) {
    for (ULONG i = 0; i < pdrgpcrGroupingCols->Size(); i++) {
      const CColRef* pcrGroupingCol = (*pdrgpcrGroupingCols)[i];
      std::string colName = cexpressiontoboss::utils::WStringToString(
          pcrGroupingCol->Name().Pstr()->GetBuffer());
      groupingColsArgs.push_back(Symbol{colName});
      requiredColumns.insert(colName);
    }
  }

  // Create the grouping columns list
  return std::make_pair(ComplexExpression {
      "By"_, {}, std::move(groupingColsArgs), {}}, requiredColumns);
}

Expression CPhysicalHashAggToBOSSHashAgg::GetIsDeduplicate(
    const CExpression* expr) {
  COperator::EOperatorId opid = expr->Pop()->Eopid();
  bool isDeduplicate = (opid == COperator::EopPhysicalHashAggDeduplicate);
  return "isDeduplicate"_(isDeduplicate);
}

}  // namespace cexpressiontoboss::translation