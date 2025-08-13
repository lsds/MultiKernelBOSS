#pragma once

#include "utils.hpp"

// Forward declarations
namespace cexpressiontoboss {
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class CExpressionToBOSSConverter;

namespace translation {
template <typename RetAuxType>
struct RetTypeC2B;

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class ScalarTranslatorBase {
 public:
  ScalarTranslatorBase() = default;
  virtual ~ScalarTranslatorBase() = default;
  virtual bool Match(const CExpression* expr) = 0;
  virtual RetTypeC2B<RetScalarAuxType> Translate(const CExpression* expr,
                               CExpressionToBOSSConverter<RetAuxType, InpAuxType, RetScalarAuxType, InpScalarAuxType>& converter,
                               InpScalarAuxType const& aux) = 0;
  virtual int GetPriority() = 0;
};
}  // namespace translation
}  // namespace cexpressiontoboss
