#include "translators/project.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> ProjectTranslator::Match(ComplexExpression &&bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  if (head.getName() == "Project") {
    return std::make_pair(true, ComplexExpression{std::move(head), std::move(arg1), std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false, ComplexExpression{std::move(head),std::move(arg1), std::move(arg2), std::move(arg3)});
}

RetType<EmptyStruct> ProjectTranslator::Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& _) {
  auto [head, unused1_, dyns, unused2_] = std::move(bossExpr).decompose();

  // Get underlying data
  RetType<EmptyStruct> ret = converter.Convert(std::move(dyns[0]), {});
  if (!ret.success) {
    return {nullptr, false};
  }
  CExpression* undlData = ret.expr;
  if (!undlData) {
    std::cerr << "Failed to convert underlying data in Project" << std::endl;
    throw std::runtime_error("Failed to convert underlying data in Project");
  }

  std::unordered_map<std::string, CColRef*> colMap =
      utils::CreateColumnMapping(undlData);

  // Create a map of column name to expressions
  std::map<std::string, CExpression*> colExprMap;

  // Process each 'as' statement in dyns (starting from index 1)
  for (size_t i = 1; i < dyns.size(); i++) {
    // Check if it's a complex expression
    if (!std::holds_alternative<ComplexExpression>(dyns[i])) {
      continue;
    }

    ComplexExpression asExpr = std::get<ComplexExpression>(std::move(dyns[i]));
    std::map<std::string, Expression> columnExprMap;

    // Extract column names and value expressions
    if (!utils::ExtractAsExpression(std::move(asExpr), columnExprMap)) {
      continue;
    }

    // Convert each scalar expression in the map
    for (auto& [outColName, valueExpr] : columnExprMap) {
      // Convert the scalar expression
      CExpression* pexprInput;
      try {
        auto ret = converter.ConvertScalar(std::move(valueExpr), colMap);
        if (!ret.success) {
          utils::safeRelease(pexprInput, undlData);
          for (auto& [_, cexpr] : colExprMap) {
            utils::safeRelease(cexpr);
          }
          return {nullptr, false};
        }

        pexprInput = ret.expr;
        if (!pexprInput) {
          std::cerr << "Failed to convert scalar expression" << std::endl;
          throw std::runtime_error("Failed to convert scalar expression");
        }
      } catch (const std::exception& e) {
        utils::safeRelease(pexprInput, undlData);
        for (auto& [_, cexpr] : colExprMap) {
          utils::safeRelease(cexpr);
        }
        std::cerr << "Failed to convert scalar expression: " << e.what()
                  << std::endl;
        throw std::runtime_error("Failed to convert scalar expression: " +
                                 std::string(e.what()));
      }

      colExprMap[outColName] = pexprInput;
    }
  }

  // If no columns to project, just return the underlying data
  if (colExprMap.empty()) {
    return {undlData, true};
  }

  // Create the projection list
  CExpression* pexprProjectList = utils::CreateProjectList(mp, mda, colExprMap);

  if (!pexprProjectList) {
    utils::safeRelease(undlData);
    std::cerr << "Failed to create project list" << std::endl;
    throw std::runtime_error("Failed to create project list");
  }

  // Create and return the logical project operator
  return {GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalProject(mp),
                                  undlData, pexprProjectList), true};

}

int ProjectTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
