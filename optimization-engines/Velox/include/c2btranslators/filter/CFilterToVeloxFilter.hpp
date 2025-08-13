#pragma once

#include "translation/filter/CFilterToBOSSFilter.hpp"

namespace cexpressiontoboss::translation {

class CFilterToVeloxFilter : public CFilterToBOSSFilter {
 public:
  CFilterToVeloxFilter() = default;
  ~CFilterToVeloxFilter() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
