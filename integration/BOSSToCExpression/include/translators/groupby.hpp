#pragma once
#include "Translator.hpp"

namespace bosstocexpression {

class GroupByTranslator : public Translator {
  public:
    GroupByTranslator(CMemoryPool *mp, CMDAccessor *mda) : Translator(mp, mda) {}
    virtual ~GroupByTranslator() = default;

    virtual std::pair<bool, ComplexExpression> Match(ComplexExpression &&bossExpr) override;
    virtual RetType<EmptyStruct> Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& aux) override;
    virtual int GetPriority() override;
};

}  // namespace bosstocexpression