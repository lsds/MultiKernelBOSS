#pragma once

#include "translation/agg/CPhysicalScalarAggToBOSSScalarAgg.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalScalarAggToVeloxScalarAgg
    : public CPhysicalScalarAggToBOSSScalarAgg {
 public:
  CPhysicalScalarAggToVeloxScalarAgg() = default;
  ~CPhysicalScalarAggToVeloxScalarAgg() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
