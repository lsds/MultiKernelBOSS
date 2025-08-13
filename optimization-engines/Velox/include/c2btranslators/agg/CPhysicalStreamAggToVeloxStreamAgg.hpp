#pragma once

#include "translation/agg/CPhysicalStreamAggToBOSSStreamAgg.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalStreamAggToVeloxStreamAgg
    : public CPhysicalStreamAggToBOSSStreamAgg {
 public:
  CPhysicalStreamAggToVeloxStreamAgg() = default;
  ~CPhysicalStreamAggToVeloxStreamAgg() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
