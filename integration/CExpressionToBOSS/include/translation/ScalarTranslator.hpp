#pragma once

#include "utils.hpp"
#include "ScalarTranslatorBase.hpp"
#include "translation/Translator.hpp"

// Forward declarations
namespace cexpressiontoboss {
namespace translation {
class ScalarTranslator : public ScalarTranslatorBase<EmptyStruct, ProjectInfo, ColSet, EmptyStruct> {
 public:
  ScalarTranslator() = default;
  virtual ~ScalarTranslator() = default;
  virtual bool Match(const CExpression* expr) = 0;
  virtual RetTypeC2B<ColSet> Translate(const CExpression* expr,
                               CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                               EmptyStruct const& aux) = 0;
  virtual int GetPriority() = 0;
};
}  // namespace translation
}  // namespace cexpressiontoboss
