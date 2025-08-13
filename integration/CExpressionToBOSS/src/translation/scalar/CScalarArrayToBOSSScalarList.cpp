#include "translation/scalar/CScalarArrayToBOSSScalarList.hpp"

#include "CExpressionToBOSS.hpp"
#include "gpopt/operators/CScalarArray.h"

namespace cexpressiontoboss::translation {

bool CScalarArrayToBOSSScalarList::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarArray;
}

RetTypeC2B<ColSet> CScalarArrayToBOSSScalarList::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  ExpressionArguments args;

  const ULONG arity = expr->Arity();
  for (ULONG i = 0; i < arity; i++) {
    CExpression* childExpr = (*expr)[i];
    RetTypeC2B<ColSet> childBossExpr = converter.ConvertScalar(childExpr, aux);
    success &= childBossExpr.success;
    args.push_back(std::move(childBossExpr.expr));
    requiredColumns.insert(childBossExpr.aux.begin(), childBossExpr.aux.end());
  }
  return {ComplexExpression{"List"_, {}, std::move(args), {}}, success, requiredColumns};
}

int CScalarArrayToBOSSScalarList::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation