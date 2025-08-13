#include "c2btranslators/agg/CPhysicalStreamAggToVeloxStreamAgg.hpp"

#include "CExpressionToBOSS.hpp"
namespace cexpressiontoboss::translation {
RetTypeC2B<EmptyStruct> CPhysicalStreamAggToVeloxStreamAgg::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> projectList = GetProjectList(expr, converter);
  success &= projectList.success;
  newRequiredColumns.insert(projectList.aux.begin(), projectList.aux.end());

  auto [groupingColumns, groupingColumnsRequiredColumns] = GetGroupingColumns(expr);
  newRequiredColumns.insert(groupingColumnsRequiredColumns.begin(), groupingColumnsRequiredColumns.end());

  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  RetTypeC2B<EmptyStruct> childExpr = utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  success &= childExpr.success;

  return {"StreamAgg"_(
      std::move(childExpr.expr),
      std::move(projectList.expr),
      std::move(groupingColumns),
      GetIsDeduplicate(expr)), success};
}

int CPhysicalStreamAggToVeloxStreamAgg::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation