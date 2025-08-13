#include "translators/orderby.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> OrderByTranslator::Match(ComplexExpression &&bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  if (head.getName() == "Order") {
    return std::make_pair(true, ComplexExpression{std::move(head), std::move(arg1), std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false, ComplexExpression{std::move(head),std::move(arg1), std::move(arg2), std::move(arg3)});
}

RetType<EmptyStruct> OrderByTranslator::Translate(
    ComplexExpression&& bossExpr,
    BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct,
                               ColRefMap>& converter,
    EmptyStruct const& _) {
  auto [head, unused1_, dyns, unused2_] = std::move(bossExpr).decompose();

  RetType<EmptyStruct> ret = converter.Convert(std::move(dyns[0]), {});
  if (!ret.success) {
    return {nullptr, false};
  }
  CExpression* childExpr = ret.expr;
  if (!childExpr) {
    std::cerr << "Failed to convert child expression in OrderBy" << std::endl;
    throw std::runtime_error("Failed to convert child expression in OrderBy");
  }

  std::unordered_map<std::string, CColRef*> colMap =
      utils::CreateColumnMapping(childExpr);

  COrderSpec* pos = GPOS_NEW(mp) COrderSpec(mp);

  // Check if we have a "By" expression for ordering specification
  if (dyns.size() > 1 &&
      std::holds_alternative<boss::expressions::ComplexExpression>(dyns[1])) {
    auto& orderExpr = std::get<boss::expressions::ComplexExpression>(dyns[1]);
    try {
      utils::ProcessOrderByExpression(mp, orderExpr, pos, colMap);
    } catch (const std::exception& e) {
      utils::safeRelease(childExpr, pos);
      std::cerr << "Failed to process order by expression in OrderBy: "
                << e.what() << std::endl;
      throw std::runtime_error(
          "Failed to process order by expression in OrderBy: " +
          std::string(e.what()));
    }
  } else {
    std::cerr << "Expected 'By' head for ordering specification in OrderBy"
              << std::endl;
    throw std::runtime_error(
        "Expected 'By' head for ordering specification in OrderBy");
  }

  CExpression* offsetExpr = CUtils::PexprScalarConstInt8(mp, 0);
  CExpression* limitExpr =
      CUtils::PexprScalarConstInt8(mp, gpos::ulong_max);  // No real limit

  CLogicalLimit* limitOp =
      GPOS_NEW(mp) CLogicalLimit(mp, pos, true, true, false);

  return {GPOS_NEW(mp)
      CExpression(mp, limitOp, childExpr, offsetExpr, limitExpr), true};
}

int OrderByTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
