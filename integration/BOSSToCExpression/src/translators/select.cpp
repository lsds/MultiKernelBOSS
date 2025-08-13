#include "translators/select.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> SelectTranslator::Match(ComplexExpression &&bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  if (head.getName() == "Select") {
    return std::make_pair(true, ComplexExpression{std::move(head), std::move(arg1), std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false, ComplexExpression{std::move(head),std::move(arg1), std::move(arg2), std::move(arg3)});
}

RetType<EmptyStruct> SelectTranslator::Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& _) {
  auto [head, unused1_, dyns, unused2_] = std::move(bossExpr).decompose();

  // Get underlying data (table)
  RetType<EmptyStruct> ret = converter.Convert(std::move(dyns[0]), {});
  if (!ret.success) {
    return {nullptr, false};
  }
  CExpression* undlData = ret.expr;
  if (!undlData) {
    std::cerr << "Failed to convert underlying data expression in Select"
              << std::endl;
    throw std::runtime_error(
        "Failed to convert underlying data expression in Select");
  }

  // Ensure we have a "Where" component
  if (!std::holds_alternative<ComplexExpression>(dyns[1])) {
    utils::safeRelease(undlData);
    std::cerr << "Where clause in Select must be a complex expression"
              << std::endl;
    throw std::runtime_error(
        "Expected ComplexExpression for 'Where' in Select");
  }

  // Extract the "Where" component
  auto whereExpr = std::get<ComplexExpression>(std::move(dyns[1]));
  auto [whereHead, where___, whereArgs, where____] =
      std::move(whereExpr).decompose();

  // Validate that it's actually a "Where" component
  if (whereHead.getName() != "Where") {
    utils::safeRelease(undlData);
    std::cerr << "Expected 'Where' head for condition in Select" << std::endl;
    throw std::runtime_error("Expected 'Where' head for condition in Select");
  }

  // Ensure the Where component has exactly one argument (the condition)
  if (whereArgs.size() != 1) {
    utils::safeRelease(undlData);
    std::cerr
        << "'Where' expression must have exactly one argument (the condition)"
        << std::endl;
    throw std::runtime_error(
        "'Where' expression must have exactly one argument (the condition)");
  }

  // Extract the condition from the Where component
  Expression condition = std::move(whereArgs[0]);

  // Extract the columns from the underlying relation and create column mapping
  std::unordered_map<std::string, CColRef*> colMap =
      utils::CreateColumnMapping(undlData);

  // Now convert the condition predicate using the column mapping
  CExpression* pexprPredicate = nullptr;
  try {
    RetType<EmptyStruct> ret = converter.ConvertScalar(std::move(condition), colMap);
    if (!ret.success) {
      utils::safeRelease(undlData);
      return {nullptr, false};
    }
    pexprPredicate = ret.expr;

    if (!pexprPredicate) {
      std::cerr << "Failed to convert where clause predicate in Select"
                << std::endl;
      throw std::runtime_error(
          "Failed to convert where clause predicate in Select");
    }

    // Create the logical select operator
    return {GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
                                    undlData, pexprPredicate), true};
  } catch (const std::exception& e) {
    // Clean up resources if an error occurs
    utils::safeRelease(undlData, pexprPredicate);
    std::cerr << "Failed to convert where clause predicate in Select: "
              << e.what() << std::endl;
    throw;
  }
}

int SelectTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
