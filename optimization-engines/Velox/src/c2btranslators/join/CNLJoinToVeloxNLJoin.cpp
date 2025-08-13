#include "c2btranslators/join/CNLJoinToVeloxNLJoin.hpp"

#include "CExpressionToBOSS.hpp"
namespace cexpressiontoboss::translation {
RetTypeC2B<EmptyStruct> CNLJoinToVeloxNLJoin::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> joinCondExpr = GetJoinCondExpr(expr, converter);
  success &= joinCondExpr.success;
  newRequiredColumns.insert(joinCondExpr.aux.begin(), joinCondExpr.aux.end());

  COperator::EOperatorId opid = expr->Pop()->Eopid();

  // Create the BOSS expression based on the NL join type
  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  std::pair<RetTypeC2B<EmptyStruct>, RetTypeC2B<EmptyStruct>> children =
      utils::GetChildExprBinary(expr, converter, newAux);
  RetTypeC2B<EmptyStruct> outerChild = std::move(children.first);
  RetTypeC2B<EmptyStruct> innerChild = std::move(children.second);
  success &= outerChild.success;
  success &= innerChild.success;

  Expression joinExpr;
  switch (opid) {
    case COperator::EopPhysicalInnerNLJoin:
    case COperator::EopPhysicalInnerIndexNLJoin:
      joinExpr = "InnerNLJoin"_(
          std::move(outerChild.expr), std::move(innerChild.expr),
          std::move(joinCondExpr.expr),
          GetUseIndexExpr(opid));
      break;
    case COperator::EopPhysicalLeftOuterNLJoin:
    case COperator::EopPhysicalLeftOuterIndexNLJoin:
      joinExpr = "LeftOuterNLJoin"_(
          std::move(outerChild.expr), std::move(innerChild.expr),
          std::move(joinCondExpr.expr),
          GetUseIndexExpr(opid));
      break;

    case COperator::EopPhysicalLeftSemiNLJoin:
      joinExpr = "LeftSemiNLJoin"_(
          std::move(outerChild.expr), std::move(innerChild.expr),
          std::move(joinCondExpr.expr));
      break;

    case COperator::EopPhysicalLeftAntiSemiNLJoin:
      joinExpr = "LeftAntiSemiNLJoin"_(
          std::move(outerChild.expr), std::move(innerChild.expr),
          std::move(joinCondExpr.expr));
      break;

    case COperator::EopPhysicalLeftAntiSemiNLJoinNotIn:
      joinExpr = "LeftAntiSemiNLJoinNotIn"_(
          std::move(outerChild.expr), std::move(innerChild.expr),
          std::move(joinCondExpr.expr));
      break;

    default:
      throw std::runtime_error("Unsupported NL join type");
  }

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

int CNLJoinToVeloxNLJoin::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
