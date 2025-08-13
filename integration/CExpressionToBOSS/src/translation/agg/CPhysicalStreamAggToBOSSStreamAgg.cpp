#include "translation/agg/CPhysicalStreamAggToBOSSStreamAgg.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
bool CPhysicalStreamAggToBOSSStreamAgg::Match(const CExpression* expr) {
  COperator::EOperatorId opid = expr->Pop()->Eopid();
  return (opid == COperator::EopPhysicalStreamAgg ||
          opid == COperator::EopPhysicalStreamAggDeduplicate);
}

RetTypeC2B<ColSet> CPhysicalStreamAggToBOSSStreamAgg::GetProjectList(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  CExpression* pexprProjList = (*expr)[1];
  return converter.ConvertScalar(pexprProjList, {});
}

std::pair<Expression, ColSet> CPhysicalStreamAggToBOSSStreamAgg::GetGroupingColumns(
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

Expression CPhysicalStreamAggToBOSSStreamAgg::GetIsDeduplicate(
    const CExpression* expr) {
  COperator::EOperatorId opid = expr->Pop()->Eopid();
  bool isDeduplicate = (opid == COperator::EopPhysicalStreamAggDeduplicate);
  return "isDeduplicate"_(isDeduplicate);
}

}  // namespace cexpressiontoboss::translation