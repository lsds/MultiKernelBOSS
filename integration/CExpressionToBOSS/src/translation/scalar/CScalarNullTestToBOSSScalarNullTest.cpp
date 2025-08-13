#include "translation/scalar/CScalarNullTestToBOSSScalarNullTest.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CScalarNullTestToBOSSScalarNullTest::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarNullTest;
}

RetTypeC2B<ColSet> CScalarNullTestToBOSSScalarNullTest::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  CExpression* childExpr = (*expr)[0];
  RetTypeC2B<ColSet> childBossExpr = converter.ConvertScalar(childExpr, aux);
  success &= childBossExpr.success;
  requiredColumns.insert(childBossExpr.aux.begin(), childBossExpr.aux.end());
  return {"IsNull"_(std::move(childBossExpr.expr)), success, requiredColumns};
}

int CScalarNullTestToBOSSScalarNullTest::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation