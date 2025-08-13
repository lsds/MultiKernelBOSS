#pragma once
#include "ScalarTranslator.hpp"

namespace bosstocexpression {

class RegularTypeTranslator : public ScalarTranslator {
  public:
    RegularTypeTranslator(CMemoryPool *mp, CMDAccessor *mda) : ScalarTranslator(mp, mda) {}
    virtual ~RegularTypeTranslator() = default;

    virtual std::pair<bool, Expression> Match(Expression &&bossExpr) override;
    virtual RetType<EmptyStruct> Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) override;
    virtual int GetPriority() override;
};

}  // namespace bosstocexpression