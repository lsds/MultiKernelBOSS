#include "c2btranslators/filter/CFilterToVeloxFilter.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
RetTypeC2B<EmptyStruct> CFilterToVeloxFilter::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> predicate = GetPredicateExpr(expr, converter);
  success &= predicate.success;
  newRequiredColumns.insert(predicate.aux.begin(), predicate.aux.end());
  if (std::move(predicate.expr) == true) {
    ProjectInfo newAux;
    newAux.parentOp = expr->Pop()->Eopid();
    newAux.requiredColumns = newRequiredColumns;
    return utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  }

  RetTypeC2B<ColSet> predicate2 = GetPredicateExpr(expr, converter);
  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  RetTypeC2B<EmptyStruct> childExpr = utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  success &= childExpr.success;

  auto selectExpr = "Select"_(
      std::move(childExpr.expr),
      "Where"_(std::move(predicate2.expr)));

  if (!(aux.parentOp == COperator::EopPhysicalComputeScalar)) {
    ColSet outputColumns = utils::GetOutputColumns(const_cast<CExpression*>(expr));
    ColSet projectColumns = utils::GetSetIntersection(outputColumns, aux.requiredColumns);
    if (projectColumns != outputColumns) {
      Expression projectList = utils::CreateProjectListFromColumns(projectColumns);
      return {"Project"_(std::move(selectExpr), std::move(projectList)), success};
    }
  }

  return {std::move(selectExpr), success};
}

int CFilterToVeloxFilter::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
