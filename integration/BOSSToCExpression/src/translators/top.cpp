#include "translators/top.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> TopTranslator::Match(ComplexExpression &&bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  if (head.getName() == "Top") {
    return std::make_pair(true, ComplexExpression{std::move(head), std::move(arg1), std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false, ComplexExpression{std::move(head),std::move(arg1), std::move(arg2), std::move(arg3)});
}

RetType<EmptyStruct> TopTranslator::Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& _) {
  auto [head, unused1_, dyns, unused2_] = std::move(bossExpr).decompose();

  RetType<EmptyStruct> ret = converter.Convert(std::move(dyns[0]), {});
  if (!ret.success) {
    return {nullptr, false};
  }
  CExpression* childExpr = ret.expr;
  if (!childExpr) {
    std::cerr << "Failed to convert child expression in Top" << std::endl;
    throw std::runtime_error("Failed to convert child expression in Top");
  }

  std::unordered_map<std::string, CColRef*> colMap =
      utils::CreateColumnMapping(childExpr);

  // Create the order specification
  COrderSpec* pos = GPOS_NEW(mp) COrderSpec(mp);

  // Check if we have an ordering specification ('By')
  if (dyns.size() > 2 &&
      std::holds_alternative<boss::expressions::ComplexExpression>(dyns[1])) {
    auto& orderExpr = std::get<boss::expressions::ComplexExpression>(dyns[1]);
    try {
      utils::ProcessOrderByExpression(mp, orderExpr, pos, colMap);
    } catch (const std::exception& e) {
      utils::safeRelease(childExpr, pos);
      std::cerr << "Failed to process order by expression in Top: " << e.what()
                << std::endl;
      throw;
    }
  }

  // Get limit expression from last argument
  CExpression* limitExpr = nullptr;
  try {
    // The limit is the second argument if no ordering, or the third if ordering
    // is present
    size_t limitIndex =
        (dyns.size() > 2 &&
         std::holds_alternative<boss::expressions::ComplexExpression>(dyns[1]))
            ? 2
            : 1;
    RetType<EmptyStruct> ret = converter.ConvertScalar(std::move(dyns[limitIndex]), colMap);
    if (!ret.success) {
      utils::safeRelease(childExpr, pos);
      return {nullptr, false};
    }
    limitExpr = ret.expr;
    if (!limitExpr) {
      std::cerr << "Failed to convert limit expression in Top" << std::endl;
      throw std::runtime_error("Failed to convert limit expression in Top");
    }
  } catch (const std::exception& e) {
    utils::safeRelease(childExpr, pos, limitExpr);
    std::cerr << "Failed to convert limit expression in Top: " << e.what()
              << std::endl;
    throw;
  }

  CExpression* offsetExpr = CUtils::PexprScalarConstInt8(mp, 0);

  CLogicalLimit* limitOp =
      GPOS_NEW(mp) CLogicalLimit(mp, pos, true, true, false);

  return {GPOS_NEW(mp)
      CExpression(mp, limitOp, childExpr, offsetExpr, limitExpr), true};
}

int TopTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
