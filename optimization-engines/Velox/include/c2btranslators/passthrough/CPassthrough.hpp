#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CPassthrough : public Translator {
 public:
  CPassthrough() = default;
  ~CPassthrough() override = default;
  bool Match(const CExpression* expr) override;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
