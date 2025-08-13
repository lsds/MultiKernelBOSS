#include "translators/scalar/comparisonop.hpp"
#include "Translator.hpp"

namespace bosstocexpression {

std::pair<bool, Expression> ComparisonOpTranslator::Match(Expression &&bossExpr) {
  return utils::ScalarComplexExpressionMatches(std::move(bossExpr), {"Equal", "StringContainsQ", "Greater", "Less", "GreaterEqual", "LessEqual", "NotEqual"});
}

RetType<EmptyStruct> ComparisonOpTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> RetType<EmptyStruct> {
            auto [head, __, args, ___] = std::move(cexpr).decompose();
            const std::string& opType = head.getName();
            if (args.size() != 2) {
              std::cerr << "Comparison operator requires exactly 2 arguments"
                        << std::endl;
              throw std::runtime_error(
                  "Comparison operator requires exactly 2 arguments");
            }

            CExpression* pexprLeft = nullptr;
            CExpression* pexprRight = nullptr;

            try {
              RetType<EmptyStruct> retLeft = converter.ConvertScalar(std::move(args[0]), colMap);
              RetType<EmptyStruct> retRight = converter.ConvertScalar(std::move(args[1]), colMap);

              if (!(retLeft.success && retRight.success)) {
                utils::safeRelease(retLeft.expr, retRight.expr);
                return {nullptr, false};
              }

              pexprLeft = retLeft.expr;
              pexprRight = retRight.expr;

              if (!pexprLeft || !pexprRight) {
                utils::safeRelease(pexprLeft, pexprRight);
                std::cerr << "Failed to convert comparison operands"
                          << std::endl;
                throw std::runtime_error(
                    "Failed to convert comparison operands");
              }

              IMDType::ECmpType cmpType;
              if (opType == "Equal" || opType == "StringContainsQ")
                cmpType = IMDType::EcmptEq;
              else if (opType == "Greater")
                cmpType = IMDType::EcmptG;
              else if (opType == "Less")
                cmpType = IMDType::EcmptL;
              else if (opType == "GreaterEqual")
                cmpType = IMDType::EcmptGEq;
              else if (opType == "LessEqual")
                cmpType = IMDType::EcmptLEq;
              else
                cmpType = IMDType::EcmptNEq;  // NotEqual
              return {CUtils::PexprScalarCmp(mp, pexprLeft, pexprRight,
                                            cmpType), true};
            } catch (const std::exception& e) {
              // Clean up memory if an exception occurs
              utils::safeRelease(pexprLeft, pexprRight);
              std::cerr << "Failed to create comparison operands"
                        << std::endl;
              throw;
            }
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int ComparisonOpTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
