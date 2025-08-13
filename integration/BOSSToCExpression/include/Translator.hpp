#pragma once
#include "TranslatorBase.hpp"
#include "b2cDefaultTypes.hpp"
namespace bosstocexpression {

class Translator : public TranslatorBase<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> {
  public:
    Translator(CMemoryPool *mp, CMDAccessor *mda) : mp(mp), mda(mda) {}
    virtual ~Translator() = default;

    virtual std::pair<bool, ComplexExpression> Match(ComplexExpression &&bossExpr) override = 0;
    virtual RetType<EmptyStruct> Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& aux) override = 0;
    virtual int GetPriority() override = 0;

  protected:
    CMemoryPool *mp;
    CMDAccessor *mda;
};

}  // namespace bosstocexpression