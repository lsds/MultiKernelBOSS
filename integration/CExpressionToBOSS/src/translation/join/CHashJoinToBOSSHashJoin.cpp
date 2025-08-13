#include "translation/join/CHashJoinToBOSSHashJoin.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CHashJoinToBOSSHashJoin::Match(const CExpression* expr) {
  if (!expr) return false;

  COperator::EOperatorId opid = expr->Pop()->Eopid();

  // Match all hash join types
  return (opid == COperator::EopPhysicalInnerHashJoin ||
          opid == COperator::EopPhysicalLeftOuterHashJoin ||
          opid == COperator::EopPhysicalLeftSemiHashJoin ||
          opid == COperator::EopPhysicalLeftAntiSemiHashJoin ||
          opid == COperator::EopPhysicalLeftAntiSemiHashJoinNotIn);
}

RetTypeC2B<ColSet> CHashJoinToBOSSHashJoin::GetJoinCondExpr(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  ColSet requiredColumns;
  CExpression* pexprJoinCond = (*expr)[2];
  RetTypeC2B<ColSet> ret = converter.ConvertScalar(pexprJoinCond, {});
  requiredColumns.insert(ret.aux.begin(), ret.aux.end());
  return {std::move(ret.expr), ret.success, requiredColumns};
}

std::pair<std::pair<Expression, Expression>, std::pair<bool, ColSet>> CHashJoinToBOSSHashJoin::GetInnerAndOuterKeyLists(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  ColSet requiredColumns;
  bool success = true;
  // Get the hash join keys using dynamic cast to access base class
  // CPhysicalHashJoin
  CPhysicalHashJoin* pHashJoin = dynamic_cast<CPhysicalHashJoin*>(expr->Pop());
  if (!pHashJoin) {
    throw std::runtime_error("Failed to cast to CPhysicalHashJoin");
  }

  // Extract outer and inner hash key expressions
  const CExpressionArray* pdrgpexprOuterKeys = pHashJoin->PdrgpexprOuterKeys();
  const CExpressionArray* pdrgpexprInnerKeys = pHashJoin->PdrgpexprInnerKeys();

  // Ensure we have matching keys
  if (pdrgpexprOuterKeys->Size() != pdrgpexprInnerKeys->Size() ||
      pdrgpexprOuterKeys->Size() == 0) {
    throw std::runtime_error("Inconsistent or empty hash join keys");
  }

  // Build arrays of join key columns for BOSS representation
  ExpressionArguments outerKeyArgs;
  ExpressionArguments innerKeyArgs;

  for (ULONG i = 0; i < pdrgpexprOuterKeys->Size(); i++) {
    CExpression* pexprOuterKey = (*pdrgpexprOuterKeys)[i];
    CExpression* pexprInnerKey = (*pdrgpexprInnerKeys)[i];

    // Convert each key expression to BOSS
    RetTypeC2B<ColSet> retOuter = converter.ConvertScalar(pexprOuterKey, {});
    success &= retOuter.success;
    requiredColumns.insert(retOuter.aux.begin(), retOuter.aux.end());
    RetTypeC2B<ColSet> retInner = converter.ConvertScalar(pexprInnerKey, {});
    success &= retInner.success;
    requiredColumns.insert(retInner.aux.begin(), retInner.aux.end());

    // Add the converted expressions to our key lists
    outerKeyArgs.push_back(std::move(retOuter.expr));
    innerKeyArgs.push_back(std::move(retInner.expr));
  }

  // Create key lists
  ComplexExpression outerKeyList{"List"_, {}, std::move(outerKeyArgs), {}};
  ComplexExpression innerKeyList{"List"_, {}, std::move(innerKeyArgs), {}};

  return std::make_pair(std::make_pair(std::move(outerKeyList), std::move(innerKeyList)), std::make_pair(success, requiredColumns));
}

}  // namespace cexpressiontoboss::translation
