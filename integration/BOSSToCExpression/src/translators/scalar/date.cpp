#include "translators/scalar/date.hpp"
#include <iomanip>
#include <ctime>
#include <chrono>
#include "Translator.hpp"

namespace bosstocexpression {


std::pair<bool, Expression> DateTranslator::Match(Expression &&bossExpr) {
  return utils::ScalarComplexExpressionMatches(std::move(bossExpr), {"DateObject", "Year"});
}

int BOSSD2I(std::string str) {
  std::istringstream iss;
  iss.str(std::string(str));
  struct std::tm tm = {};
  iss >> std::get_time(&tm, "%Y-%m-%d");
  auto t = std::mktime(&tm);
  static int const hoursInADay = 24;
  return (int32_t)(std::chrono::duration_cast<std::chrono::hours>(
                           std::chrono::system_clock::from_time_t(t).time_since_epoch())
                           .count() /
                            hoursInADay);
}


RetType<EmptyStruct> DateTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> RetType<EmptyStruct> {
            auto [head, __, args, ___] = std::move(cexpr).decompose();
            const std::string& opType = head.getName();
            if (opType == "DateObject") {
              if (args.size() != 1) {
                std::cerr << "DateObject operator requires exactly 1 argument"
                          << std::endl;
                throw std::runtime_error(
                    "DateObject operator requires exactly 1 argument");
              }

              // Extract the date string from the argument
              std::string dateStr;

              // Check if the argument is a string
              if (std::holds_alternative<std::string>(args[0])) {
                dateStr = std::get<std::string>(args[0]);
              } else {
                std::cerr << "DateObject argument must be a string literal"
                          << std::endl;
                throw std::runtime_error(
                    "DateObject argument must be a string literal");
              }

              try {
                int dateValue = BOSSD2I(dateStr);
                // turn date into an int
                return {CUtils::PexprScalarConstInt4(mp, dateValue), true};
                // return {utils::CreateGenericType(mp, GPDB_DATE_OID, &dateValue, sizeof(int),
                //                          dateValue, CDouble(dateValue)), true};
              } catch (const std::exception& e) {
                std::cerr << "Failed to parse date: " << dateStr
                          << ". Error: " << e.what() << std::endl;
                throw std::runtime_error(std::string("Failed to parse date: ") +
                                         dateStr + ". Error: " + e.what());
              }
            } else { // Year operator
              if (args.size() != 1) {
                std::cerr << "Year operator requires exactly 1 argument"
                          << std::endl;
                throw std::runtime_error(
                    "Year operator requires exactly 1 argument");
              }

              CExpression* pexpr = nullptr;

              try {
                RetType<EmptyStruct> ret = converter.ConvertScalar(std::move(args[0]), colMap);
                if (!ret.success) {
                  return {nullptr, false};
                }
                pexpr = ret.expr;
                return {utils::CreateUnaryScalarOp(mp,
                    pexpr, OID(GPDB_INT4_YEAR_OP), GPOS_WSZ_LIT("Year")), true};
              } catch (const std::exception& e) {
                // Clean up memory if an exception occurs
                utils::safeRelease(pexpr);
                std::cerr << "Failed to create Year operator" << std::endl;
                throw;
              }
            }
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int DateTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
