#pragma once

#include "translation/scan/CScanToBOSSGetColumns.hpp"

namespace cexpressiontoboss::translation {

class CScanToVeloxGetColumns : public CScanToBOSSGetColumns {
 public:
  CScanToVeloxGetColumns() = default;
  ~CScanToVeloxGetColumns() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
