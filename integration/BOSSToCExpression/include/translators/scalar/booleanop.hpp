#pragma once
#include "ScalarTranslator.hpp"

namespace bosstocexpression {

class BooleanOpTranslator : public ScalarTranslator {
  public:
    BooleanOpTranslator(CMemoryPool *mp, CMDAccessor *mda) : ScalarTranslator(mp, mda) {}
    virtual ~BooleanOpTranslator() = default;

    virtual std::pair<bool, Expression> Match(Expression &&bossExpr) override;
    virtual RetType<EmptyStruct> Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) override;
    virtual int GetPriority() override;
};

}  // namespace bosstocexpression