#pragma once
#include "ScalarTranslator.hpp"

namespace bosstocexpression {

class ComparisonOpTranslator : public ScalarTranslator {
  public:
    ComparisonOpTranslator(CMemoryPool *mp, CMDAccessor *mda) : ScalarTranslator(mp, mda) {}
    virtual ~ComparisonOpTranslator() = default;

    virtual std::pair<bool, Expression> Match(Expression &&bossExpr) override;
    virtual RetType<EmptyStruct> Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) override;
    virtual int GetPriority() override;
};

}  // namespace bosstocexpression