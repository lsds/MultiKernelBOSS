#include "translators/scalar/stringcontainsq.hpp"
#include "Translator.hpp"

namespace bosstocexpression {


std::pair<bool, Expression> StringContainsQTranslator::Match(Expression &&bossExpr) {
  return {false, std::move(bossExpr)};
  // return utils::ScalarComplexExpressionMatches(std::move(bossExpr), {"StringContainsQ"});
}

RetType<EmptyStruct> StringContainsQTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> RetType<EmptyStruct> {
            auto [head, __, args, ___] = std::move(cexpr).decompose();
            const std::string& opType = head.getName();
            if (args.size() != 2) {
              std::cerr
                  << "StringContainsQ operator requires exactly 2 arguments"
                  << std::endl;
              throw std::runtime_error(
                  "StringContainsQ operator requires exactly 2 arguments");
            }

            CExpression* pexprLeft = nullptr;
            CExpression* pexprRight = nullptr;

            try {
              RetType<EmptyStruct> retLeft = converter.ConvertScalar(std::move(args[0]), colMap);
              RetType<EmptyStruct> retRight = converter.ConvertScalar(std::move(args[1]), colMap);
              if (!retLeft.success || !retRight.success) {
                utils::safeRelease(retLeft.expr, retRight.expr);
                return {nullptr, false};
              }
              pexprLeft = retLeft.expr;
              pexprRight = retRight.expr;
              return {utils::CreateBinaryScalarOp(mp,
                    pexprLeft, pexprRight,
                                          GPDB_INT4_STRINGCONTAINSQ_OP,
                                          GPOS_WSZ_LIT("StringContainsQ")), true};
            } catch (const std::exception& e) {
              // Clean up memory if an exception occurs
              utils::safeRelease(pexprLeft, pexprRight);
              std::cerr << "Failed to create binary operation operands"
                        << std::endl;
              throw;
            }
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int StringContainsQTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
