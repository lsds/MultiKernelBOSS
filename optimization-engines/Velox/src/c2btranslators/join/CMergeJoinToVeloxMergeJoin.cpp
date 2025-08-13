#include "c2btranslators/join/CMergeJoinToVeloxMergeJoin.hpp"

#include "CExpressionToBOSS.hpp"
namespace cexpressiontoboss::translation {

RetTypeC2B<EmptyStruct> CMergeJoinToVeloxMergeJoin::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> joinCondExpr = GetJoinCondExpr(expr, converter);
  success &= joinCondExpr.success;
  newRequiredColumns.insert(joinCondExpr.aux.begin(), joinCondExpr.aux.end());

  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  std::pair<RetTypeC2B<EmptyStruct>, RetTypeC2B<EmptyStruct>> children =
      utils::GetChildExprBinary(expr, converter, newAux);
  RetTypeC2B<EmptyStruct> outerChild = std::move(children.first);
  RetTypeC2B<EmptyStruct> innerChild = std::move(children.second);
  success &= outerChild.success;
  success &= innerChild.success;

  auto joinExpr = "FullOuterMergeJoin"_(
      std::move(outerChild.expr), std::move(innerChild.expr),
      std::move(joinCondExpr.expr));

  if (!(aux.parentOp == COperator::EopPhysicalComputeScalar)) {
    ColSet outputColumns = utils::GetOutputColumns(const_cast<CExpression*>(expr));
    ColSet projectColumns = utils::GetSetIntersection(outputColumns, aux.requiredColumns);
    if (projectColumns != outputColumns) {
      Expression projectList = utils::CreateProjectListFromColumns(projectColumns);
      return {"Project"_(std::move(joinExpr), std::move(projectList)), success};
    }
  }
  return {std::move(joinExpr), success};
}

int CMergeJoinToVeloxMergeJoin::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation
