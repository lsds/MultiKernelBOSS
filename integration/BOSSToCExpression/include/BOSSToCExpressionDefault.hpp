#pragma once

#include "BOSSToCExpression.hpp"
#include "b2cDefaultTypes.hpp"

namespace bosstocexpression {

template <typename RetAuxType>
struct RetType;

class BOSSToCExpressionDefaultConverter : public BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap>{
 public:
  BOSSToCExpressionDefaultConverter() = default;
  ~BOSSToCExpressionDefaultConverter() = default;

  virtual std::pair<CExpression *, bool> ConvertExpr(Expression&& bossExpr) override {
    RetType<EmptyStruct> ret = Convert(std::move(bossExpr), {});
    return std::make_pair(ret.expr, ret.success);
  };


};
}  // namespace bosstocexpression
