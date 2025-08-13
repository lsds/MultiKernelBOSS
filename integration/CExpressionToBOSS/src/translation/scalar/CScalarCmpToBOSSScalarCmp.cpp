#include "translation/scalar/CScalarCmpToBOSSScalarCmp.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CScalarCmpToBOSSScalarCmp::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarCmp;
}

RetTypeC2B<ColSet> CScalarCmpToBOSSScalarCmp::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  CScalarCmp* scalarCmp = CScalarCmp::PopConvert(expr->Pop());

  // Get comparison operation
  IMDType::ECmpType cmpType = scalarCmp->ParseCmpType();

  // Convert the left and right expressions
  CExpression* leftExpr = (*expr)[0];
  CExpression* rightExpr = (*expr)[1];
  RetTypeC2B<ColSet> leftBossExpr = converter.ConvertScalar(leftExpr, aux);
  RetTypeC2B<ColSet> rightBossExpr = converter.ConvertScalar(rightExpr, aux);
  success &= leftBossExpr.success;
  success &= rightBossExpr.success;
  requiredColumns.insert(leftBossExpr.aux.begin(), leftBossExpr.aux.end());
  requiredColumns.insert(rightBossExpr.aux.begin(), rightBossExpr.aux.end());

  // Create the appropriate comparison operation
  switch (cmpType) {
    case IMDType::EcmptEq:
      if (leftBossExpr.expr == rightBossExpr.expr) {
        return {true, success, requiredColumns};
      }
      return {"Equal"_(std::move(leftBossExpr.expr), std::move(rightBossExpr.expr)), success, requiredColumns};
    case IMDType::EcmptNEq:
      return {"NotEqual"_(std::move(leftBossExpr.expr), std::move(rightBossExpr.expr)), success, requiredColumns};
    case IMDType::EcmptL:
      return {"Greater"_(std::move(rightBossExpr.expr), std::move(leftBossExpr.expr)), success, requiredColumns};
    case IMDType::EcmptLEq:
      return {"LessEqual"_(std::move(leftBossExpr.expr), std::move(rightBossExpr.expr)), success, requiredColumns};
    case IMDType::EcmptG:
      return {"Greater"_(std::move(leftBossExpr.expr), std::move(rightBossExpr.expr)), success, requiredColumns};
    case IMDType::EcmptGEq:
      return {"GreaterEqual"_(std::move(leftBossExpr.expr), std::move(rightBossExpr.expr)), success, requiredColumns};
    default:
      throw std::runtime_error("Unsupported comparison type");
  }
}

int CScalarCmpToBOSSScalarCmp::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation