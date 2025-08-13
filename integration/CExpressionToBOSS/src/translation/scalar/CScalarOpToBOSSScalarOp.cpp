#include "translation/scalar/CScalarOpToBOSSScalarOp.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CScalarOpToBOSSScalarOp::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarOp;
}

RetTypeC2B<ColSet> CScalarOpToBOSSScalarOp::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  CScalarOp* scalarOp = CScalarOp::PopConvert(expr->Pop());

  // Get the operator's metadata ID
  const IMDId* mdid = scalarOp->MdIdOp();
  std::string operatorName =
      "Plus";  // Default to Plus if operator cannot be identified

  int children = 2;
  // Check if the mdid is valid
  if (mdid != nullptr && mdid->IsValid()) {
    // Try to get the OID - we need to be careful with the casting
    ULONG oid = 0;

    // Check if it's a GPDB ID by trying a dynamic cast
    const CMDIdGPDB* gpdbId = dynamic_cast<const CMDIdGPDB*>(mdid);
    if (gpdbId != nullptr) {
      oid = gpdbId->Oid();

      // Match OID to operator name
      switch (oid) {
        case 551:  // INT4 addition
          operatorName = "Plus";
          break;
        case 552:  // INT4 subtraction
          operatorName = "Minus";
          break;
        case 594:  // INT4 multiplication
          operatorName = "Times";
          break;
        case 691:  // INT4 division
          operatorName = "Divide";
          break;
        // Add more cases as needed for other operators
        case 1380:  // INT4 year
          operatorName = "Year";
          children = 1;
          break;
        case 1381:
          operatorName = "StringContainsQ";
          break;
        default:
          // Keep default "Plus" for unrecognized operators
          throw std::runtime_error("Unsupported operator");
      }
    }
  } else {
    throw std::runtime_error("Unsupported operator");
  }

  // Convert the left and right expressions

  std::vector<Expression> args;
  for (int i = 0; i < children; i++) {
    CExpression* childExpr = (*expr)[i];
    RetTypeC2B<ColSet> childBossExpr = converter.ConvertScalar(childExpr, aux);
    success &= childBossExpr.success;
    args.push_back(std::move(childBossExpr.expr));
    requiredColumns.insert(childBossExpr.aux.begin(), childBossExpr.aux.end());
  }

  // Create the appropriate BOSS expression based on the operator
  // Use the string literal directly with the _ operator
  if (operatorName == "Plus") {
    return {"Plus"_(std::move(args[0]), std::move(args[1])), success, requiredColumns};
  } else if (operatorName == "Minus") {
    return {"Minus"_(std::move(args[0]), std::move(args[1])), success, requiredColumns};
  } else if (operatorName == "Times") {
    return {"Times"_(std::move(args[0]), std::move(args[1])), success, requiredColumns};
  } else if (operatorName == "Divide") {
    return {"Divide"_(std::move(args[0]), std::move(args[1])), success, requiredColumns};
  } else if (operatorName == "Year") {
    return {"Year"_(std::move(args[0])), success, requiredColumns};
  } else if (operatorName == "StringContainsQ") {
    return {"StringContainsQ"_(std::move(args[0]), std::move(args[1])), success, requiredColumns};
  } else {
    // Fallback to Plus for any unknown operators
    throw std::runtime_error("Unsupported operator");
  }
}

int CScalarOpToBOSSScalarOp::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation