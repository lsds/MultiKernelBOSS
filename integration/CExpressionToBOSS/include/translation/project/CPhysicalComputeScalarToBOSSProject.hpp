#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalComputeScalarToBOSSProject : public Translator {
 public:
  CPhysicalComputeScalarToBOSSProject() = default;
  ~CPhysicalComputeScalarToBOSSProject() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for ComputeScalar
  static RetTypeC2B<ColSet> GetProjectListExpr(const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter);
};

}  // namespace cexpressiontoboss::translation
