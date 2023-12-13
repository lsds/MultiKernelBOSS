#pragma once
#include "Engine.hpp"
#include "Expression.hpp"

extern "C" {
struct BOSSSymbol {
  boss::Symbol delegate;
};
BOSSSymbol* symbolNameToNewBOSSSymbol(char const* i);
char const* symbolToNewString(BOSSSymbol const* arg);

struct BOSSExpression {
  boss::Expression delegate;
};
BOSSExpression* intToNewBOSSExpression(int32_t i);
BOSSExpression* longToNewBOSSExpression(int64_t i);
BOSSExpression* doubleToNewBOSSExpression(double i);
BOSSExpression* stringToNewBOSSExpression(char const* i);
BOSSExpression* symbolNameToNewBOSSExpression(char const* i);

BOSSExpression* newComplexBOSSExpression(BOSSSymbol* head, size_t cardinality,
                                         BOSSExpression* arguments[]);

/**
 *     bool = 0, long = 1, double = 2 , std::string = 3, Symbol = 4 , ComplexExpression = 5
 */
size_t getBOSSExpressionTypeID(BOSSExpression const* arg);

bool getBoolValueFromBOSSExpression(BOSSExpression const* arg);
std::int32_t getIntValueFromBOSSExpression(BOSSExpression const* arg);
std::int64_t getLongValueFromBOSSExpression(BOSSExpression const* arg);
std::double_t getDoubleValueFromBOSSExpression(BOSSExpression const* arg);
char* getNewStringValueFromBOSSExpression(BOSSExpression const* arg);
char const* getNewSymbolNameFromBOSSExpression(BOSSExpression const* arg);

BOSSSymbol* getHeadFromBOSSExpression(BOSSExpression const* arg);
size_t getArgumentCountFromBOSSExpression(BOSSExpression const* arg);
BOSSExpression** getArgumentsFromBOSSExpression(BOSSExpression const* arg);

BOSSExpression* BOSSEvaluate(BOSSExpression* arg);
void freeBOSSExpression(BOSSExpression* e);
void freeBOSSArguments(BOSSExpression** e);
void freeBOSSSymbol(BOSSSymbol* s);
void freeBOSSString(char* s);
}

namespace boss {
expressions::Expression evaluate(expressions::Expression&& e);
}
