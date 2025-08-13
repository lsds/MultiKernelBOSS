#pragma once

#include "translation/join/CNLJoinToBOSSNLJoin.hpp"

namespace cexpressiontoboss::translation {

class CNLJoinToVeloxNLJoin : public CNLJoinToBOSSNLJoin {
 public:
  CNLJoinToVeloxNLJoin() = default;
  ~CNLJoinToVeloxNLJoin() override = default;
  RetTypeC2B<EmptyStruct> Translate(const CExpression* expr,
                       CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter,
                       ProjectInfo const& aux) override;
  int GetPriority() override;
};

}  // namespace cexpressiontoboss::translation
