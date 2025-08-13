#pragma once
#include "BOSSToCExpression.hpp"

namespace bosstocexpression {
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class BOSSToCExpressionConverter;

template <typename RetAuxType>
struct RetType {
  gpopt::CExpression *expr;
  bool success;
  RetAuxType aux;
};

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class TranslatorBase {
  public:
    TranslatorBase() = default;
    virtual ~TranslatorBase() = default;

    virtual std::pair<bool, ComplexExpression> Match(ComplexExpression &&bossExpr) = 0;
    virtual RetType<RetAuxType> Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<RetAuxType, InpAuxType, RetScalarAuxType, InpScalarAuxType> &converter, InpAuxType const& aux) = 0;
    virtual int GetPriority() = 0;
};

}  // namespace bosstocexpression