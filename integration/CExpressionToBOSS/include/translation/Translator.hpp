#pragma once

#include "utils.hpp"
#include "TranslatorBase.hpp"
#include "c2bDefaultTypes.hpp"

namespace cexpressiontoboss {
namespace translation {

class Translator : public TranslatorBase<EmptyStruct, ProjectInfo, ColSet, EmptyStruct> {
 public:
  Translator() = default;
  virtual ~Translator() override = default;
  virtual bool Match(const CExpression* expr) override = 0;
  virtual RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                               CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                               ProjectInfo const& aux) override = 0;
  virtual int GetPriority() override = 0;
};
}  // namespace translation
}  // namespace cexpressiontoboss
