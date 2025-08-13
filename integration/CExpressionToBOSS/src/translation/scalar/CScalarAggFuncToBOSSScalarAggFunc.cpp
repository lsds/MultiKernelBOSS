#include "translation/scalar/CScalarAggFuncToBOSSScalarAggFunc.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
bool CScalarAggFuncToBOSSScalarAggFunc::Match(const CExpression* expr) {
  return (expr->Pop()->Eopid() == COperator::EopScalarAggFunc);
}

RetTypeC2B<ColSet> CScalarAggFuncToBOSSScalarAggFunc::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  // Get the scalar aggregate function operator
  CScalarAggFunc* pScalarAggFunc = dynamic_cast<CScalarAggFunc*>(expr->Pop());
  if (!pScalarAggFunc) {
    throw std::runtime_error("Failed to cast to CScalarAggFunc");
  }

  // Get the aggregate function name
  const CWStringConst* pstrAggFunc = pScalarAggFunc->PstrAggFunc();
  if (!pstrAggFunc) {
    throw std::runtime_error("Aggregate function name is null");
  }

  std::string aggFuncName =
      cexpressiontoboss::utils::WStringToString(pstrAggFunc->GetBuffer());
  // Check if this is a distinct aggregate
  bool isDistinct = pScalarAggFunc->IsDistinct();

  // Convert the function arguments
  ExpressionArguments args;
  for (ULONG i = 0; i < expr->Arity(); i++) {
    CExpression* pexprArg = (*expr)[i];
    RetTypeC2B<ColSet> nestedExpr = converter.ConvertScalar(pexprArg, aux);
    args.push_back(std::move(nestedExpr.expr));
    success &= nestedExpr.success;
    requiredColumns.insert(nestedExpr.aux.begin(), nestedExpr.aux.end());
  }

  // Special handling for distinct aggregate
  if (isDistinct) {
    // Create a distinct wrapper for the arguments
    Expression distinctArgs =
        ComplexExpression{"Distinct"_, {}, std::move(args), {}};

    // Create and return the aggregate function directly with the distinct wrapper
    ExpressionArguments aggArgs;
    aggArgs.push_back(std::move(distinctArgs));

    return {ComplexExpression{Symbol{aggFuncName}, {}, std::move(aggArgs), {}}, success, requiredColumns};
  } else {
    // Handle COUNT(*) special case
    if (aggFuncName == "Count" && args.size() == 0) {
      args.push_back("*"_);
    }

    // Create and return the aggregate function directly with regular arguments
    return {ComplexExpression{Symbol{aggFuncName}, {}, std::move(args), {}}, success, requiredColumns};
  }
}

int CScalarAggFuncToBOSSScalarAggFunc::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation