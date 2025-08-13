#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalScalarAggToBOSSScalarAgg : public Translator {
 public:
  CPhysicalScalarAggToBOSSScalarAgg() = default;
  ~CPhysicalScalarAggToBOSSScalarAgg() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for ScalarAgg
  static RetTypeC2B<ColSet> GetProjectList(const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
};

}  // namespace cexpressiontoboss::translation
