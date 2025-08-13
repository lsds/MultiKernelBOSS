#pragma once

#include "translation/agg/CPhysicalHashAggToBOSSHashAgg.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalHashAggToVeloxHashAgg
    : public CPhysicalHashAggToBOSSHashAgg {
 public:
  CPhysicalHashAggToVeloxHashAgg() = default;
  ~CPhysicalHashAggToVeloxHashAgg() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};
}  // namespace cexpressiontoboss::translation
