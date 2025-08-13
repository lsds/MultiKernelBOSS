#pragma once

#include "translation/sort/CSortToBOSSSort.hpp"

namespace cexpressiontoboss::translation {

class CSortToVeloxSort : public CSortToBOSSSort {
 public:
  CSortToVeloxSort() = default;
  ~CSortToVeloxSort() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
