#pragma once
#include "Translator.hpp"

namespace bosstocexpression {

class GetColumnsTranslator : public Translator {
  public:
    GetColumnsTranslator(CMemoryPool *mp, CMDAccessor *mda) : Translator(mp, mda) {}
    virtual ~GetColumnsTranslator() = default;

    virtual std::pair<bool, ComplexExpression> Match(ComplexExpression &&bossExpr) override;
    virtual RetType<EmptyStruct> Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& aux) override;
    virtual int GetPriority() override;
};

}  // namespace bosstocexpression