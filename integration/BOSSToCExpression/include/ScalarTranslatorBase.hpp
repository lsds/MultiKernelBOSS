#pragma once
// Forward declarations instead of includes
#include "utils.hpp" // For Expression type

namespace bosstocexpression {
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class BOSSToCExpressionConverter;

template <typename RetAuxType>
struct RetType;

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class ScalarTranslatorBase {
  public:
    ScalarTranslatorBase() = default;
    virtual ~ScalarTranslatorBase() = default;

    virtual std::pair<bool, Expression> Match(Expression &&bossExpr) = 0;
    virtual RetType<RetScalarAuxType> Translate(Expression &&bossExpr, BOSSToCExpressionConverter<RetAuxType, InpAuxType, RetScalarAuxType, InpScalarAuxType> &converter, InpScalarAuxType const& aux) = 0;
    virtual int GetPriority() = 0;
};

}  // namespace bosstocexpression