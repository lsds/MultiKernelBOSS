#pragma once

#include "translation/project/CPhysicalComputeScalarToBOSSProject.hpp"

namespace cexpressiontoboss::translation {

class CPhysicalComputeScalarToVeloxProject
    : public CPhysicalComputeScalarToBOSSProject {
 public:
  CPhysicalComputeScalarToVeloxProject() = default;
  ~CPhysicalComputeScalarToVeloxProject() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
