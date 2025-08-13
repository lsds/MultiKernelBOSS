#include "translators/scalar/booleanop.hpp"
#include "Translator.hpp"

namespace bosstocexpression {

std::pair<bool, Expression> BooleanOpTranslator::Match(Expression &&bossExpr) {
  return utils::ScalarComplexExpressionMatches(std::move(bossExpr), {"And", "Or", "Not"});
}

RetType<EmptyStruct> BooleanOpTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> RetType<EmptyStruct> {
            auto [head, __, args, ___] = std::move(cexpr).decompose();
            const std::string& opType = head.getName();
            // Handle boolean operators
            if (opType == "And" || opType == "Or") {
              if (args.size() < 2) {
                std::cerr << "Boolean operator requires at least 2 arguments"
                          << std::endl;
                throw std::runtime_error(
                    "Boolean operator requires at least 2 arguments");
              }

              CExpressionArray* pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
              try {
                for (auto& arg : args) {
                  RetType<EmptyStruct> ret =
                      converter.ConvertScalar(std::move(arg), colMap);
                  if (!ret.success) {
                    utils::safeRelease(pdrgpexpr);
                    return {nullptr, false};
                  }
                  if (!ret.expr) {
                    utils::safeRelease(pdrgpexpr);
                    std::cerr << "Failed to convert boolean operator child"
                              << std::endl;
                    throw std::runtime_error(
                        "Failed to convert boolean operator child");
                  }
                  pdrgpexpr->Append(ret.expr);
                }

                CScalarBoolOp::EBoolOperator boolop =
                    (opType == "And") ? CScalarBoolOp::EboolopAnd
                                      : CScalarBoolOp::EboolopOr;

                return {GPOS_NEW(mp) CExpression(
                    mp, GPOS_NEW(mp) CScalarBoolOp(mp, boolop), pdrgpexpr), true};
              } catch (const std::exception& e) {
                // Clean up memory if an exception occurs
                pdrgpexpr->Release();
                std::cerr << "Failed to create boolean operator" << std::endl;
                throw;
              }
            } else { // Not operator
              if (args.size() != 1) {
                std::cerr << "Not operator requires exactly 1 argument"
                          << std::endl;
                throw std::runtime_error(
                    "Not operator requires exactly 1 argument");
              }

              CExpression* pexprChild = nullptr;
              try {
                RetType<EmptyStruct> ret = converter.ConvertScalar(std::move(args[0]), colMap);

                if (!ret.success) {
                  return {nullptr, false};
                }

                pexprChild = ret.expr;
                if (!pexprChild) {
                  std::cerr << "Failed to convert Not operator child"
                            << std::endl;
                  throw std::runtime_error(
                      "Failed to convert Not operator child");
                }

                return {GPOS_NEW(mp) CExpression(
                    mp,
                    GPOS_NEW(mp) CScalarBoolOp(mp, CScalarBoolOp::EboolopNot),
                    pexprChild), true};
              } catch (const std::exception& e) {
                // Clean up memory if an exception occurs
                utils::safeRelease(pexprChild);
                std::cerr << "Failed to create Not operator" << std::endl;
                throw;
              }
            }
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int BooleanOpTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
