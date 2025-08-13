#pragma once

#include "translation/project/CPhysicalComputeScalarToBOSSProject.hpp"

namespace cexpressiontoboss::translation {

class CGPUProjectToAFProject : public Translator {
 public:
  CGPUProjectToAFProject() = default;
  ~CGPUProjectToAFProject() override = default;
  bool Match(const CExpression* expr) override;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
