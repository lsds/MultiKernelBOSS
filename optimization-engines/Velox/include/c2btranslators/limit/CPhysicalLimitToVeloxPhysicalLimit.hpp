#pragma once

#include "translation/limit/CPhysicalLimitToBOSSPhysicalLimit.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalLimitToVeloxPhysicalLimit
    : public CPhysicalLimitToBOSSPhysicalLimit {
 public:
  CPhysicalLimitToVeloxPhysicalLimit() = default;
  ~CPhysicalLimitToVeloxPhysicalLimit() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
