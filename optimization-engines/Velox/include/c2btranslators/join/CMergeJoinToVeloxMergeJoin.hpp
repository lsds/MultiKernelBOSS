#pragma once

#include "translation/join/CMergeJoinToBOSSMergeJoin.hpp"

namespace cexpressiontoboss::translation {

class CMergeJoinToVeloxMergeJoin : public CMergeJoinToBOSSMergeJoin {
 public:
  CMergeJoinToVeloxMergeJoin() = default;
  ~CMergeJoinToVeloxMergeJoin() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
