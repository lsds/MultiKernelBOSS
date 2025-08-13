#pragma once

#include "translation/filter/CFilterToBOSSFilter.hpp"

namespace cexpressiontoboss::translation {

class CGPUFilterToAFFilter : public Translator {
 public:
  CGPUFilterToAFFilter() = default;
  ~CGPUFilterToAFFilter() override = default;
  bool Match(const CExpression* expr) override;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
