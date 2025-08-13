#pragma once
// #include <BOSS.hpp>
#include "utils.hpp"

// Forward declarations
namespace cexpressiontoboss {
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class CExpressionToBOSSConverter;

namespace translation {
template <typename RetAuxType>
struct RetTypeC2B {
  boss::Expression expr;
  bool success;
  RetAuxType aux;
};

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class TranslatorBase {
 public:
  TranslatorBase() = default;
  virtual ~TranslatorBase() = default;
  virtual bool Match(const CExpression* expr) = 0;
  virtual RetTypeC2B<RetAuxType> Translate(const CExpression* expr,
                               CExpressionToBOSSConverter<RetAuxType, InpAuxType, RetScalarAuxType, InpScalarAuxType>& converter,
                               InpAuxType const& aux) = 0;
  virtual int GetPriority() = 0;
};
}  // namespace translation
}  // namespace cexpressiontoboss
