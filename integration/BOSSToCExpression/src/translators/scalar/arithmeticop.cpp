#include "translators/scalar/arithmeticop.hpp"
#include "Translator.hpp"

namespace bosstocexpression {
std::pair<bool, Expression> ArithmeticOpTranslator::Match(Expression &&bossExpr) {
  return utils::ScalarComplexExpressionMatches(std::move(bossExpr), {"Plus", "Minus", "Multiply", "Times", "Divide"});
}

RetType<EmptyStruct> ArithmeticOpTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> RetType<EmptyStruct> {
            auto [head, __, args, ___] = std::move(cexpr).decompose();
            const std::string& opType = head.getName();
            // Handle arithmetic operators
            if (opType == "Plus") {
              if (args.size() != 2) {
                std::cerr << "Plus operator requires exactly 2 arguments"
                          << std::endl;
                throw std::runtime_error(
                    "Plus operator requires exactly 2 arguments");
              }

              CExpression* pexprLeft = nullptr;
              CExpression* pexprRight = nullptr;

              try {
                RetType<EmptyStruct> left = converter.ConvertScalar(std::move(args[0]), colMap);
                RetType<EmptyStruct> right = converter.ConvertScalar(std::move(args[1]), colMap);
                if (!left.success || !right.success) {
                  utils::safeRelease(left.expr, right.expr);
                  return {nullptr, false};
                }
                pexprLeft = left.expr;
                pexprRight = right.expr;
                return {utils::CreateBinaryScalarOp(mp,
                    pexprLeft, pexprRight, GPDB_INT4_ADD_OP, GPOS_WSZ_LIT("+")), true};
              } catch (const std::exception& e) {
                // Clean up memory if an exception occurs
                utils::safeRelease(pexprLeft, pexprRight);
                throw;
              }
            } else if (opType == "Minus") {
              if (args.size() != 2) {
                std::cerr << "Minus operator requires exactly 2 arguments"
                          << std::endl;
                throw std::runtime_error(
                    "Minus operator requires exactly 2 arguments");
              }

              CExpression* pexprLeft = nullptr;
              CExpression* pexprRight = nullptr;

              try {
                RetType<EmptyStruct> left = converter.ConvertScalar(std::move(args[0]), colMap);
                RetType<EmptyStruct> right = converter.ConvertScalar(std::move(args[1]), colMap);
                if (!left.success || !right.success) {
                  utils::safeRelease(left.expr, right.expr);
                  return {nullptr, false};
                }
                pexprLeft = left.expr;
                pexprRight = right.expr;
                return {utils::CreateBinaryScalarOp(mp,
                    pexprLeft, pexprRight, GPDB_INT4_SUB_OP, GPOS_WSZ_LIT("-")), true};
              } catch (const std::exception& e) {
                // Clean up memory if an exception occurs
                utils::safeRelease(pexprLeft, pexprRight);
                std::cerr << "Failed to create binary operation operands"
                          << std::endl;
                throw;
              }
            } else if (opType == "Multiply" || opType == "Times") {
              if (args.size() != 2) {
                std::cerr << "Multiply operator requires exactly 2 arguments"
                          << std::endl;
                throw std::runtime_error(
                    "Multiply operator requires exactly 2 arguments");
              }

              CExpression* pexprLeft = nullptr;
              CExpression* pexprRight = nullptr;

              try {
                RetType<EmptyStruct> left = converter.ConvertScalar(std::move(args[0]), colMap);
                RetType<EmptyStruct> right = converter.ConvertScalar(std::move(args[1]), colMap);
                if (!left.success || !right.success) {
                  utils::safeRelease(left.expr, right.expr);
                  return {nullptr, false};
                }
                pexprLeft = left.expr;
                pexprRight = right.expr;
                return {utils::CreateBinaryScalarOp(mp,
                    pexprLeft, pexprRight, GPDB_INT4_MUL_OP, GPOS_WSZ_LIT("*")), true};
              } catch (const std::exception& e) {
                // Clean up memory if an exception occurs
                utils::safeRelease(pexprLeft, pexprRight);
                std::cerr << "Failed to create binary operation operands"
                          << std::endl;
                throw;
              }
            } else { // Divide operator
              if (args.size() != 2) {
                std::cerr << "Divide operator requires exactly 2 arguments"
                          << std::endl;
                throw std::runtime_error(
                    "Divide operator requires exactly 2 arguments");
              }

              CExpression* pexprLeft = nullptr;
              CExpression* pexprRight = nullptr;

              try {
                RetType<EmptyStruct> left = converter.ConvertScalar(std::move(args[0]), colMap);
                RetType<EmptyStruct> right = converter.ConvertScalar(std::move(args[1]), colMap);
                if (!left.success || !right.success) {
                  utils::safeRelease(left.expr, right.expr);
                  return {nullptr, false};
                }
                pexprLeft = left.expr;
                pexprRight = right.expr;
                return {utils::CreateBinaryScalarOp(mp, 
                    pexprLeft, pexprRight, GPDB_INT4_DIV_OP, GPOS_WSZ_LIT("/")), true};
              } catch (const std::exception& e) {
                // Clean up memory if an exception occurs
                utils::safeRelease(pexprLeft, pexprRight);
                std::cerr << "Failed to create binary operation operands"
                          << std::endl;
                throw;
              }
            }
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int ArithmeticOpTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
