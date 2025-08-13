#include "translators/join.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> JoinTranslator::Match(
    ComplexExpression&& bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  std::vector<std::string> op_list = {
      "Join",         "LeftJoin",          "OuterJoin", "SemiJoin",
      "AntiSemiJoin", "AntiSemiJoinNotIn", "NAryJoin"};
  if (std::find(op_list.begin(), op_list.end(), head.getName()) !=
      op_list.end()) {
    return std::make_pair(true,
                          ComplexExpression{std::move(head), std::move(arg1),
                                            std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false,
                        ComplexExpression{std::move(head), std::move(arg1),
                                          std::move(arg2), std::move(arg3)});
}

RetType<EmptyStruct> JoinTranslator::Translate(
    ComplexExpression&& bossExpr,
    BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct,
                               ColRefMap>& converter,
    EmptyStruct const& _) {
  auto [head, unused1_, dyns, unused2_] = std::move(bossExpr).decompose();

  // Handle N-ary join
  std::unordered_map<std::string, CColRef*>
      colMap;  // Make this non-const so we can modify it

  if (head.getName() == "NAryJoin") {
    // Create arrays to hold child expressions and join conditions
    CExpressionArray* children = GPOS_NEW(mp) CExpressionArray(mp);

    // First argument should be an array of tables
    if (!std::holds_alternative<ComplexExpression>(dyns[0])) {
      utils::safeRelease(children);
      std::cerr << "First argument of NAryJoin must be a complex expression"
                << std::endl;
      throw std::runtime_error(
          "First argument of NAryJoin must be a complex expression");
    }

    ComplexExpression tables = std::get<ComplexExpression>(std::move(dyns[0]));
    auto [tablesHead, ___, tableArgs, ____] = std::move(tables).decompose();
    // Convert each table expression
    for (auto&& arg : tableArgs) {
      RetType<EmptyStruct> ret = converter.Convert(std::move(arg), {});
      if (!ret.success) {
        utils::safeRelease(children);
        return {nullptr, false};
      }
      CExpression* child = ret.expr;

      if (!child) {
        utils::safeRelease(children);
        std::cerr << "Failed to convert table expression in NAryJoin"
                  << std::endl;
        throw std::runtime_error(
            "Failed to convert table expression in NAryJoin");
      }

      auto childColMap = utils::CreateColumnMapping(child);
      colMap.insert(childColMap.begin(), childColMap.end());

      children->Append(child);
    }
    // Second argument should be the join conditions
    CExpression* joinCondition = nullptr;
    if (std::holds_alternative<ComplexExpression>(dyns[1])) {
      ComplexExpression conditions =
          std::get<ComplexExpression>(std::move(dyns[1]));
      auto [condHead, _____, condArgs, ______] =
          std::move(conditions).decompose();
      // Convert join conditions into a single AND expression
      for (auto&& arg : condArgs) {
        CExpression* cond;
        try {
          RetType<EmptyStruct> ret =
              converter.ConvertScalar(std::move(arg), colMap);
          if (!ret.success) {
            utils::safeRelease(joinCondition, children);
            return {nullptr, false};
          }
          cond = ret.expr;
        } catch (const std::exception& e) {
          utils::safeRelease(cond, joinCondition, children);
          std::cerr << "Failed to convert join condition: " << e.what()
                    << std::endl;
          throw;
        }
        if (!cond) {  // no cond is just constant true.
          continue;
        }
        if (!joinCondition) {
          joinCondition = cond;
        } else {
          joinCondition = GPOS_NEW(mp) CExpression(
              mp, GPOS_NEW(mp) CScalarBoolOp(mp, CScalarBoolOp::EboolopAnd),
              joinCondition, cond);
        }
      }
    }

    // If no valid join condition was created, create a constant TRUE condition
    // for cross product
    if (!joinCondition) {
      joinCondition = CUtils::PexprScalarConstBool(mp, true);
    }

    children->Append(joinCondition);
    return {GPOS_NEW(mp)
                CExpression(mp, GPOS_NEW(mp) CLogicalNAryJoin(mp), children),
            true};
  }

  // Handle binary joins
  CExpression* pexprLeft;
  CExpression* pexprRight;
  try {
    RetType<EmptyStruct> retLeft = converter.Convert(std::move(dyns[0]), {});
    RetType<EmptyStruct> retRight = converter.Convert(std::move(dyns[1]), {});

    if (!retLeft.success || !retRight.success) {
      utils::safeRelease(pexprLeft, pexprRight);
      return {nullptr, false};
    }
    pexprLeft = retLeft.expr;
    pexprRight = retRight.expr;

    if (!pexprLeft || !pexprRight) {
      std::cerr << "Failed to convert join operands" << std::endl;
      throw std::runtime_error("Failed to convert join operands");
    }
  } catch (const std::exception& e) {
    utils::safeRelease(pexprLeft, pexprRight);
    std::cerr << "Failed to convert join operands: " << e.what() << std::endl;
    throw;
  }

  // Get output columns from this child and create a mapping
  CColRefSet* outputColsLeft = pexprLeft->DeriveOutputColumns();
  CColRefSet* outputColsRight = pexprRight->DeriveOutputColumns();

  auto childColMapLeft = utils::CreateColumnMapping(pexprLeft);
  auto childColMapRight = utils::CreateColumnMapping(pexprRight);

  colMap.insert(childColMapLeft.begin(), childColMapLeft.end());
  colMap.insert(childColMapRight.begin(), childColMapRight.end());

  // Get the join condition
  CExpression* pexprJoinCondition = nullptr;
  if (std::holds_alternative<ComplexExpression>(dyns[2])) {
    ComplexExpression whereClause =
        std::get<ComplexExpression>(std::move(dyns[2]));
    auto [whereHead, __, whereArgs, ___] = std::move(whereClause).decompose();

    if (whereHead.getName() == "Where" && whereArgs.size() == 1 &&
        std::holds_alternative<ComplexExpression>(whereArgs[0])) {
      ComplexExpression conditionExpr =
          std::get<ComplexExpression>(std::move(whereArgs[0]));
      try {
        RetType<EmptyStruct> ret =
            converter.ConvertScalar(std::move(conditionExpr), colMap);
        if (!ret.success) {
          utils::safeRelease(pexprLeft, pexprRight);
          return {nullptr, false};
        }
        pexprJoinCondition = ret.expr;
      } catch (const std::exception& e) {
        utils::safeRelease(pexprLeft, pexprRight, pexprJoinCondition);
        std::cerr << "Failed to convert join condition: " << e.what()
                  << std::endl;
        throw;
      }
    } else {
      utils::safeRelease(pexprLeft, pexprRight);
      std::cerr << "Invalid join condition: " << whereHead.getName()
                << std::endl;
      throw;
    }
  }

  // If no valid join condition was provided, create a constant TRUE condition
  // for cross product
  if (!pexprJoinCondition) {
    pexprJoinCondition = CUtils::PexprScalarConstBool(mp, true);
  }

  // Create the appropriate join operator based on the head name
  if (head.getName() == "Join") {
    return {GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
                                     pexprLeft, pexprRight, pexprJoinCondition),
            true};
  } else if (head.getName() == "LeftJoin") {
    return {GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalLeftOuterJoin(mp),
                                     pexprLeft, pexprRight, pexprJoinCondition),
            true};
  } else if (head.getName() == "OuterJoin") {
    return {GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalFullOuterJoin(mp),
                                     pexprLeft, pexprRight, pexprJoinCondition),
            true};
  } else if (head.getName() == "SemiJoin") {
    return {GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalLeftSemiJoin(mp),
                                     pexprLeft, pexprRight, pexprJoinCondition),
            true};
  } else if (head.getName() == "AntiSemiJoin") {
    return {GPOS_NEW(mp)
                CExpression(mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
                            pexprLeft, pexprRight, pexprJoinCondition),
            true};
  } else if (head.getName() == "AntiSemiJoinNotIn") {
    return {GPOS_NEW(mp)
                CExpression(mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoinNotIn(mp),
                            pexprLeft, pexprRight, pexprJoinCondition),
            true};
  } else {
    utils::safeRelease(pexprLeft, pexprRight, pexprJoinCondition);
    std::cout << "Unsupported join type: " + head.getName() << std::endl;
    throw std::runtime_error("Unsupported join type: " + head.getName());
  }
}

int JoinTranslator::GetPriority() { return 0; }

}  // namespace bosstocexpression
