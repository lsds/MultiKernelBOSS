#include "c2btranslators/sort/CSortToVeloxSort.hpp"

#include "CExpressionToBOSS.hpp"
namespace cexpressiontoboss::translation {
RetTypeC2B<EmptyStruct> CSortToVeloxSort::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());
  auto [sortKeyListExpr, sortKeyListExprRequiredColumns] = GetSortKeyList(expr);
  newRequiredColumns.insert(sortKeyListExprRequiredColumns.begin(), sortKeyListExprRequiredColumns.end());

  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  RetTypeC2B<EmptyStruct> childExpr = utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  return {"Sort"_(
      std::move(childExpr.expr),
      std::move(sortKeyListExpr)), childExpr.success};
}

int CSortToVeloxSort::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
