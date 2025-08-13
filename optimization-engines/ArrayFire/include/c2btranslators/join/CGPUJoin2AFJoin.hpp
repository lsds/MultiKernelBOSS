#pragma once

#include "translation/join/CHashJoinToBOSSHashJoin.hpp"

namespace cexpressiontoboss::translation {

class CGPUJoin2AFJoin : public Translator {
 public:
  CGPUJoin2AFJoin() = default;
  ~CGPUJoin2AFJoin() override = default;
  bool Match(const CExpression* expr) override;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
