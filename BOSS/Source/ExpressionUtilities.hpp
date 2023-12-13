#pragma once
#include "Expression.hpp"
#include "Utilities.hpp"
#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>

namespace boss::utilities {
template <typename ExpressionSystem = DefaultExpressionSystem> class ExtensibleExpressionBuilder {
  Symbol const s;

public:
  explicit ExtensibleExpressionBuilder(Symbol const& s) : s(s){};
  explicit ExtensibleExpressionBuilder(const ::std::string& s) : s(Symbol(s)){};
  /**
   * This thing is a bit hacky: when construction Expression, some standard
   * libraries convert char const* to int or bool, not to ::std::string -- so I do
   * it explicitly
   */
  template <typename T>
  typename ExpressionSystem::Expression convertConstCharToStringAndOnToExpression(T&& v) const {
    if constexpr(std::is_same_v<std::decay_t<T>, char const*>) {
      return ::std::string((char const*)v);
    }
    // also convert int32 to int64 as a convenience
    // (except if the expression's type system includes int32)
    else if constexpr(std::is_same_v<std::decay_t<T>, int> && !isAtom<int>::value) {
      return int64_t(v);
    } else {
      return std::forward<T>(v);
    }
  }

  template <typename Ts>
  using isAtom = isVariantMember<::std::decay_t<Ts>, typename ExpressionSystem::AtomicExpression>;
  template <typename Ts>
  using isComplexExpression =
      isInstanceOfTemplate<Ts, ExpressionSystem::template ComplexExpressionWithStaticArguments>;
  template <typename Ts>
  using isStaticArgument = ::std::disjunction<isComplexExpression<Ts>, isAtom<Ts>>;
  template <typename T> using isSpanArgument = isInstanceOfTemplate<T, Span>;
  template <typename T>
  using isSpanOfConstArgument = isInstanceOfTemplateWithConstArguments<T, Span>;

  template <typename T>
  using isDynamicArgument =
      ::std::conjunction<::std::negation<isStaticArgument<T>>, ::std::negation<isSpanArgument<T>>>;

  /**
   * build expression from dynamic arguments (or no arguments)
   */
  template <typename... Ts>
  ::std::enable_if_t<sizeof...(Ts) == 0 || std::disjunction_v<isDynamicArgument<Ts>...>,
                     typename ExpressionSystem::ComplexExpression>
  operator()(Ts&&... args /*a*/) const {
    typename ExpressionSystem::ExpressionArguments argList;
    argList.reserve(sizeof...(Ts));
    (argList.push_back(convertConstCharToStringAndOnToExpression<decltype(args)>(
         ::std::forward<decltype(args)>(args))),
     ...);
    return {s, {}, ::std::move(argList)};
  }

  /**
   * build expression from static arguments, some of which are expressions themselves (passing
   * arguments by rvalue reference)
   */
  template <typename... Ts>
  ::std::enable_if_t<(sizeof...(Ts) > 0) &&
                         !(std::conjunction_v<isSpanArgument<::std::decay_t<Ts>>...>)&&!(
                             std::disjunction_v<isDynamicArgument<Ts>...>),
                     typename ExpressionSystem::template ComplexExpressionWithStaticArguments<
                         ::std::decay_t<Ts>...>>
  operator()(Ts&&... args /*a*/) const {
    return {s, ::std::tuple<::std::decay_t<Ts>...>(::std::forward<Ts>(args)...)};
  };

  /**
   * build expression from span arguments
   */
  template <typename... Ts>
  ::std::enable_if_t<::std::conjunction<isSpanArgument<::std::decay_t<Ts>>...>::value &&
                         (sizeof...(Ts) > 0),
                     typename ExpressionSystem::ComplexExpression>
  operator()(Ts&&... args) const {
    auto spans = std::array{
        std::move(args)...}; // unfortunately, vectors cannot be initialized with move-only types
                             // which is why we need to put spans into an array first
    return {s, {}, {}, {std::move_iterator(begin(spans)), std::move_iterator(end(spans))}};
  }

  friend typename ExpressionSystem::Expression
  operator|(typename ExpressionSystem::Expression const& expression,
            ExtensibleExpressionBuilder const& builder) {
    return builder(expression);
  };
  operator Symbol() const { return Symbol(s); } // NOLINT
};
using ExpressionBuilder = ExtensibleExpressionBuilder<>;
static ExpressionBuilder operator""_(const char* name, size_t /*unused*/) {
  return ExpressionBuilder(name);
};

} // namespace boss::utilities
