#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalSortLimitToVeloxOrderBy : public Translator {
 public:
  CPhysicalSortLimitToVeloxOrderBy() = default;
  ~CPhysicalSortLimitToVeloxOrderBy() override = default;
  
  // Match method checks if this is a Limit with a Sort child
  bool Match(const CExpression* expr) override;
  
  // Translate method converts the expression to BOSS format
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
                       
  // EstimateCost method calculates the cost of this operator
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation 