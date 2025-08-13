#include "c2btranslators/limit/CPhysicalLimitToVeloxPhysicalLimit.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

RetTypeC2B<EmptyStruct> CPhysicalLimitToVeloxPhysicalLimit::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> limitOffsetExpr = GetOffsetExpr(expr, converter);
  success &= limitOffsetExpr.success;
  newRequiredColumns.insert(limitOffsetExpr.aux.begin(), limitOffsetExpr.aux.end());

  RetTypeC2B<ColSet> limitCountExpr = GetLimitCountExpr(expr, converter);
  success &= limitCountExpr.success;
  newRequiredColumns.insert(limitCountExpr.aux.begin(), limitCountExpr.aux.end());

  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  RetTypeC2B<EmptyStruct> childExpr = utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  success &= childExpr.success;

  return {"Top"_(
      std::move(childExpr.expr),
      std::move(limitOffsetExpr.expr),
      std::move(limitCountExpr.expr)), success};
}

int CPhysicalLimitToVeloxPhysicalLimit::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation