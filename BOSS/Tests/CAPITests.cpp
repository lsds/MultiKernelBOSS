#include <array>
#include <catch2/catch.hpp>

#include "../Source/BOSS.hpp"

TEST_CASE("Build Expression", "[api]") {
  auto input = (std::array{longToNewBOSSExpression(3), longToNewBOSSExpression(4)});
  auto* s = symbolNameToNewBOSSSymbol("Plus");
  auto* c = newComplexBOSSExpression(s, 2, input.data());
  auto* res = BOSSEvaluate(c);
  auto* result = getArgumentsFromBOSSExpression(res);
  auto secondArgument = getLongValueFromBOSSExpression(result[1]);
  freeBOSSSymbol(s);
  freeBOSSExpression(res);
  freeBOSSExpression(input[0]);
  freeBOSSExpression(input[1]);
  freeBOSSArguments(result);
  CHECK(secondArgument == 4);
}

TEST_CASE("Build expression, with strings", "[api]") {
  auto input = (std::array{stringToNewBOSSExpression("test string")});
  auto* s = symbolNameToNewBOSSSymbol("UnevaluatedAsNoEngineIsSet");
  auto* c = newComplexBOSSExpression(s, 1, input.data());
  auto* res = BOSSEvaluate(c);
  auto* result = getArgumentsFromBOSSExpression(res);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  char* argument1 = getNewStringValueFromBOSSExpression(result[0]);
  std::string str1 = std::string(argument1);
  std::string str2 = std::string("test string");
  freeBOSSSymbol(s);
  freeBOSSString(argument1);
  freeBOSSExpression(res);
  freeBOSSExpression(input[0]);
  freeBOSSArguments(result);
  CHECK(str1 == str2);
}
