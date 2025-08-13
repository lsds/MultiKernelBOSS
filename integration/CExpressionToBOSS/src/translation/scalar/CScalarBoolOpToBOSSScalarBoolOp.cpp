#include "translation/scalar/CScalarBoolOpToBOSSScalarBoolOp.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CScalarBoolOpToBOSSScalarBoolOp::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarBoolOp;
}

RetTypeC2B<ColSet> CScalarBoolOpToBOSSScalarBoolOp::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  CScalarBoolOp* scalarBool = CScalarBoolOp::PopConvert(expr->Pop());
  CScalarBoolOp::EBoolOperator boolOp = scalarBool->Eboolop();

  // Handle the NOT operator (unary)
  if (boolOp == CScalarBoolOp::EboolopNot) {
    CExpression* childExpr = (*expr)[0];
    RetTypeC2B<ColSet> childBossExpr = converter.ConvertScalar(childExpr, aux);
    success &= childBossExpr.success;
    requiredColumns.insert(childBossExpr.aux.begin(), childBossExpr.aux.end());
    if (childBossExpr.expr == true) {
      return {false, success, requiredColumns};
    } else if (childBossExpr.expr == false) {
      return {true, success, requiredColumns};
    }
    return {std::move(childBossExpr.expr), success, requiredColumns};
  }

  // handle AND/OR operators
  // Convert all children
  std::vector<Expression> vecArgs;
  for (ULONG i = 0; i < expr->Arity(); i++) {
    CExpression* childExpr = (*expr)[i];
    RetTypeC2B<ColSet> childBossExpr = converter.ConvertScalar(childExpr, aux);
    success &= childBossExpr.success;
    requiredColumns.insert(childBossExpr.aux.begin(), childBossExpr.aux.end());

    if (boolOp == CScalarBoolOp::EboolopAnd && childBossExpr.expr == false) {
      // fast path
      return {false, success, requiredColumns};
    } else if (boolOp == CScalarBoolOp::EboolopOr && childBossExpr.expr == true) {
      // fast path
      return {true, success, requiredColumns};
    }

    if (boolOp == CScalarBoolOp::EboolopAnd && childBossExpr.expr != true || boolOp == CScalarBoolOp::EboolopOr && childBossExpr.expr != false) {
      // avoid duplicates because Orca will sometimes generate them (distributed)
      if (std::find(vecArgs.begin(), vecArgs.end(), childBossExpr.expr) == vecArgs.end()) {
        vecArgs.push_back(std::move(childBossExpr.expr));
      }
    }
  }

  ExpressionArguments args;
  for (auto& arg : vecArgs) {
    args.push_back(std::move(arg));
  }

  // Create the appropriate boolean operation
  if (args.size() == 1) {
    return {std::move(args[0]), success, requiredColumns};
  }

  if (boolOp == CScalarBoolOp::EboolopAnd) {
    if (args.size() == 0) {
      return {true, success, requiredColumns};
    }
    return {ComplexExpression{"And"_, {}, std::move(args), {}}, success, requiredColumns};
  } else if (boolOp == CScalarBoolOp::EboolopOr) {
    if (args.size() == 0) {
      return {false, success, requiredColumns};
    }
    return {ComplexExpression{"Or"_, {}, std::move(args), {}}, success, requiredColumns};
  } else {
    throw std::runtime_error("Unsupported boolean operator");
  }
}

int CScalarBoolOpToBOSSScalarBoolOp::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation