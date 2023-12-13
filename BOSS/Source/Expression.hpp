#pragma once
#include "Utilities.hpp"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <variant>
#include <vector>

//#define ABLATION_NO_FAST_PATH

namespace boss {
namespace expressions {

enum class CloneReason {
  FOR_TESTING,                            // should be used only in BOSSTests!
  CONVERSION_TO_CUSTOM_EXPRESSION,        // from boss::Expression to custom Expression
  CONVERSION_TO_C_BOSS_EXPRESSION,        // from boss::Expression to C BOSSExpression
  IMPLICIT_CONVERSION_WITH_GET_ARGUMENTS, //
  FUNCTION_RETURNING_LVALUE,              //
  FUNCTION_TAKING_DEFAULT_EXPRESSION,     //
  EVALUATE_CONST_EXPRESSION, // evaluate() taking only a rvalue reference   transformations:
  EXPRESSION_WRAPPING,       // use expression as argument for another complex expression
  EXPRESSION_SUBSTITUTION,   // modifying arguments (includes argument evaluation)
  EXPRESSION_AUGMENTATION,   // adding new arguments
};
static void checkCloneWithoutReason(CloneReason reason) {}
[[deprecated("Provide a reason type instead")]] static void checkCloneWithoutReason() {}

namespace generic {
template <typename StaticArgumentsTuple, typename... AdditionalCustomAtoms>
class ComplexExpressionWithAdditionalCustomAtoms;
template <typename T>
inline constexpr bool isComplexExpression =
    boss::utilities::isInstanceOfTemplate<std::decay_t<T>,
                                          ComplexExpressionWithAdditionalCustomAtoms>::value;
} // namespace generic

namespace atoms {
class Symbol {
  std::string name;

public:
  explicit Symbol(std::string const& name) : name(name){};
  explicit Symbol(std::string&& name) : name(std::move(name)){};
  std::string const& getName() const& { return name; };
  std::string getName() && { return std::move(name); };
  inline bool operator==(Symbol const& s2) const { return getName() == s2.getName(); };
  inline bool operator!=(Symbol const& s2) const { return getName() != s2.getName(); };
  friend ::std::ostream& operator<<(::std::ostream& out, Symbol const& thing) {
    return out << thing.getName();
  }
};

template <typename Scalar> struct Span {
private: // state
  using IteratorType = std::conditional_t<
      std::is_same_v<std::remove_const_t<Scalar>, bool>,
      std::conditional_t<std::is_const_v<Scalar>, typename std::vector<bool>::const_iterator,
                         typename std::vector<bool>::iterator>,
      Scalar*>;
  IteratorType _begin = {};
  IteratorType _end = {};
  std::function<void(void)> destructor;

public: // surface
  using element_type = Scalar;
  size_t size() const { return _end - _begin; }
  constexpr auto operator[](size_t i) const -> decltype(auto) { return *(_begin + i); }
  constexpr auto operator[](size_t i) -> decltype(auto) { return *(_begin + i); }
  auto begin() const { return _begin; }
  auto end() const { return _end; }

  constexpr auto at(size_t i) const -> decltype(auto) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay,hicpp-no-array-decay)
    if(_begin + i < _end) {
      return (*this)[i];
    }
    throw std::out_of_range("Span has no element with index " + std::to_string(i));
  }
  constexpr auto at(size_t i) -> decltype(auto) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay,hicpp-no-array-decay)
    if(_begin + i < _end) {
      return (*this)[i];
    }
    throw std::out_of_range("Span has no element with index " + std::to_string(i));
  }

  constexpr Span<Scalar> subspan(size_t offset, size_t size) && {
    _begin += offset;
    _end = _begin + size;
    return std::move(*this);
  }

  constexpr Span<Scalar> subspan(size_t offset) && {
    return std::move(*this).subspan(offset, _end - _begin - offset);
  }

  /**
   * The span takes ownership of the adaptee
   */
  explicit Span(std::vector<std::remove_const_t<Scalar>>&& adaptee)
      : _begin([&/* capturing context for immediate evaluation */]() {
          if constexpr(std::is_same_v<Scalar, bool>) {
            return adaptee.begin();
          } else {
            return adaptee.data();
          }
        }()),
        _end(_begin + adaptee.size()), destructor([v = std::move(adaptee)]() {}) {}

  /**
   * The span does not take ownership of the adaptee. The vector better not be modified while the
   * span lives
   */
  explicit Span(std::vector<std::remove_const_t<Scalar>>& adaptee)
      : _begin([&adaptee]() {
          if constexpr(std::is_same_v<Scalar, bool>) {
            return adaptee.begin();
          } else {
            return adaptee.data();
          }
        }()),
        _end(_begin + adaptee.size()) {}

  /**
   * The span does not take ownership of the adaptee. The vector better not be modified while the
   * span lives
   */
  explicit Span(std::vector<std::remove_const_t<Scalar>> const& adaptee)
      : _begin([&adaptee]() {
          if constexpr(std::is_same_v<Scalar, bool>) {
            return adaptee.begin();
          } else {
            return adaptee.data();
          }
        }()),
        _end(_begin + adaptee.size()) {}

  explicit Span(IteratorType begin, size_t size, std::function<void(void)> destructor)
      : _begin(begin), _end(begin + size), destructor(std::move(destructor)) {}

  bool operator==(Span const& other) const { return _begin == other._begin; }

  Span() noexcept = default;

  /**
   * We consider Spans move-only because they can be *really* expensive to copy. It you really,
   * really have to copy one, use the clone() function and provide a reason
   */
  Span(Span const& other) = delete;
  Span(Span&& other) noexcept
      : _begin(other._begin), _end(other._end), destructor(std::move(other.destructor)) {
    other.destructor = nullptr;
  };

  /**
   * because the Span constructor cannot infer what data structure/payload was used to hold the
   * values in the other Span, arguments are copied into a std::vector. The alternative would be
   * to use some kind of reference chain or counting but I (Holger) did not like that -- I am open
   * to discussing this, though
   */
  template <typename... Reason> Span<std::remove_const_t<Scalar>> clone(Reason... reason) const& {
    checkCloneWithoutReason(reason...);
    return Span<std::remove_const_t<Scalar>>(
        std::vector<std::remove_const_t<Scalar>>(_begin, _end));
  }

  Span& operator=(Span&& other) noexcept {
    _begin = (other._begin);
    _end = (other._end);
    destructor = (std::move(other.destructor));
    other.destructor = nullptr;
    return *this;
  };

  /**
   * see comment on the copy constructor about copying Spans
   */
  Span& operator=(Span const&) = delete;
  ~Span() {
    if(destructor) {
      destructor();
    }
  };

  friend std::ostream& operator<<(std::ostream& s, Span const& span) { return s << span.size; }
};

#ifdef ABLATION_NO_FAST_PATH
static std::unordered_map<void const*, void*> globalSlowPathSpanCache;
template <class T> class MovableReferenceWrapper;
template <typename... AdditionalCustomAtoms> class ExpressionWithAdditionalCustomAtoms;
template <typename Scalar> struct SlowPathSpan {
public:
  using element_type = Scalar;
  using RetrieverFunc = std::function<Scalar(size_t)>;

private:
  RetrieverFunc retriever;
  using NonConstScalar = std::remove_const_t<Scalar>;
  NonConstScalar*& cachedArray;
  void const* basePointer;
  size_t size_;

  static NonConstScalar*& globalCache(void const* basePointer) {
    auto [it, inserted] = globalSlowPathSpanCache.try_emplace(basePointer, nullptr);
    return reinterpret_cast<NonConstScalar*&>(it->second);
  }

public:
  Scalar* baseBegin() const { return reinterpret_cast<Scalar*>(const_cast<void*>(basePointer)); }

  Scalar* begin() const {
    if(cachedArray == nullptr) {
      // copy all values into an intermediate array
      cachedArray = new NonConstScalar[size()];
      for(size_t i = 0; i < size(); ++i) {
        cachedArray[i] = at(i);
      }
    }
    return cachedArray;
  }
  Scalar* end() const { return begin() + size(); }

  auto size() const { return size_; }
  auto at(size_t i) { return retriever(i); }
  auto at(size_t i) const { return retriever(i); }
  auto operator[](size_t i) const -> decltype(auto) { return at(i); }
  auto operator[](size_t i) -> decltype(auto) { return at(i); }
  constexpr SlowPathSpan<Scalar> subspan(size_t offset) && {
    return std::move(*this).subspan(offset, size() - offset);
  }
  constexpr SlowPathSpan<Scalar> subspan(size_t offset, size_t size) && {
    if(offset > 0) {
      if(cachedArray != nullptr) {
        cachedArray += offset;
      }
      return SlowPathSpan(
          [func = std::move(retriever), offset](size_t index) { return func(index + offset); },
          size);
    }
    size_ = size;
    return std::move(*this);
  }

  template <typename Func>
  SlowPathSpan(Func&& func, size_t size, void const* basePointer)
      : retriever(std::forward<Func>(func)), size_(size), cachedArray(globalCache(basePointer)),
        basePointer(basePointer) {}

  template <typename StaticArgumentsTuple, typename... AdditionalCustomAtoms>
  SlowPathSpan(std::shared_ptr<generic::ComplexExpressionWithAdditionalCustomAtoms<
                   StaticArgumentsTuple, AdditionalCustomAtoms...>>
                   e,
               size_t beginIndex, size_t size, void const* data)
      : SlowPathSpan(
            [expr = std::move(e), beginIndex](size_t i) -> std::decay_t<Scalar> {
              using T = std::decay_t<Scalar>;
              return ::std::visit(
                  [](auto const& argument) -> T {
                    if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>,
                                                  ::std::vector<bool>::reference>) {
                      if constexpr(::std::is_same_v<::std::decay_t<T>,
                                                    ::std::vector<bool>::reference>) {

                        return argument;
                      }
                      throw ::std::bad_variant_access();
                    } else if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>::type,
                                                         T>) {
                      return argument.get();
                    } else if constexpr(boss::utilities::isInstanceOfTemplate<
                                            ::std::decay_t<decltype(argument)>,
                                            MovableReferenceWrapper>::value) {
                      if constexpr(boss::utilities::isInstanceOfTemplate<
                                       ::std::decay_t<decltype(argument.get())>,
                                       ExpressionWithAdditionalCustomAtoms>::value) {
                        return ::std::get<T>(argument.get());
                      }
                      throw ::std::bad_variant_access();
                    } else {
                      throw ::std::bad_variant_access();
                    }
                  },
                  expr->getArguments().at(beginIndex + i).getArgument());
            },
            size, data) {}

  bool operator==(SlowPathSpan const& other) const { return begin() == other.begin(); }

  SlowPathSpan(SlowPathSpan const& other) = delete;
  SlowPathSpan& operator=(SlowPathSpan const&) = delete;

  SlowPathSpan(SlowPathSpan&& other) noexcept
      : retriever(std::move(other.retriever)), cachedArray(other.cachedArray), size_(other.size_), basePointer(other.basePointer) {
    other.cachedArray = nullptr;
  }
  SlowPathSpan& operator=(SlowPathSpan&& other) noexcept {
    retriever = std::move(other.retriever);
    cachedArray = other.cachedArray;
    other.cachedArray = nullptr;
    size_ = other.size_;
    basePointer = other.basePointer;
    return *this;
  }

  ~SlowPathSpan() { delete[] cachedArray; }

  friend std::ostream& operator<<(std::ostream& s, SlowPathSpan const& span) {
    return s << span.size();
  }
};
#endif // ABLATION_NO_FAST_PATH
} // namespace atoms
using atoms::Span;
using atoms::Symbol;

template <typename TargetType> class ArgumentTypeMismatch;
template <> class ArgumentTypeMismatch<void> : public ::std::bad_variant_access {
private:
  ::std::string const whatString;

public:
  explicit ArgumentTypeMismatch(::std::string const& whatString) : whatString(whatString) {}
  const char* what() const noexcept override { return whatString.c_str(); }
};
template <typename... T> ArgumentTypeMismatch(::std::string const&) -> ArgumentTypeMismatch<void>;
template <typename TargetType> class ArgumentTypeMismatch : public ArgumentTypeMismatch<void> {
public:
  template <typename VariantType>
  explicit ArgumentTypeMismatch(VariantType const& v)
      : ArgumentTypeMismatch<void>([&v]() {
          ::std::stringstream s;
          s << "expected and actual type mismatch in expression \"";
          if(!v.valueless_by_exception()) {
            s << v;
          } else {
            s << "valueless by exception";
          }
          static auto typenames = ::std::map<::std::type_index, char const*>{
              {typeid(int32_t), "int"},     {typeid(int64_t), "long"},
              {typeid(Symbol), "Symbol"},   {typeid(bool), "bool"},
              {typeid(double_t), "double"}, {typeid(::std::string), "string"}};
          s << "\", expected "
            << (typenames.count(typeid(TargetType)) ? typenames.at(typeid(TargetType))
                                                    : typeid(TargetType).name());
          return s.str();
        }()) {}
};

template <typename... AdditionalCustomAtoms>
using AtomicExpressionWithAdditionalCustomAtoms =
    std::variant<bool, std::int32_t, std::int64_t, std::double_t, std::string, Symbol,
                 AdditionalCustomAtoms...>;

namespace generic {

template <typename... AdditionalCustomAtoms>
class ExpressionWithAdditionalCustomAtoms
    : public boss::utilities::variant_amend<
          AtomicExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>,
          ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>,
                                                     AdditionalCustomAtoms...>>::type {
public:
  using SuperType = typename boss::utilities::variant_amend<
      AtomicExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>,
      ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, AdditionalCustomAtoms...>>::type;

  using SuperType::SuperType;

  ExpressionWithAdditionalCustomAtoms(ExpressionWithAdditionalCustomAtoms const&) = delete;
  ExpressionWithAdditionalCustomAtoms&
  operator=(ExpressionWithAdditionalCustomAtoms const&) = delete;

  // allow conversion from int32_t/float_t to int64_t/double_t
  // but only if int32_t/float_t are not supported already by the AdditionalCustomAtoms
  template <
      typename T,
      typename U = std::enable_if_t<
          std::conjunction_v<std::disjunction<std::is_same<T, int32_t>, std::is_same<T, float_t>>,
                             std::negation<std::is_constructible<SuperType, T>>>,
          std::conditional_t<std::is_integral_v<T>, int64_t, double_t>>>
  explicit ExpressionWithAdditionalCustomAtoms(T v) noexcept
      : ExpressionWithAdditionalCustomAtoms(U(v)) {}

  template <typename = std::enable_if<sizeof...(AdditionalCustomAtoms) != 0>, typename... T>
  ExpressionWithAdditionalCustomAtoms( // NOLINT(hicpp-explicit-conversions)
      ExpressionWithAdditionalCustomAtoms<T...>&& o) noexcept
      : SuperType(std::visit(
            boss::utilities::overload(
                [](ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, T...>&& unpacked)
                    -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
                  return ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>,
                                                                    AdditionalCustomAtoms...>(
                      std::forward<decltype(unpacked)>(unpacked));
                },
                [](auto&& unpacked) {
                  return ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>(
                      std::forward<decltype(unpacked)>(unpacked));
                }),
            (typename boss::utilities::variant_amend<
                 AtomicExpressionWithAdditionalCustomAtoms<T...>,
                 ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, T...>>::type &&)
                std::move(o))) {}

  ~ExpressionWithAdditionalCustomAtoms() = default;
  ExpressionWithAdditionalCustomAtoms(ExpressionWithAdditionalCustomAtoms&&) noexcept = default;
  ExpressionWithAdditionalCustomAtoms&
  operator=(ExpressionWithAdditionalCustomAtoms&&) noexcept = default;

  template <typename T>
  std::enable_if_t<boss::utilities::isInstanceOfTemplate<
                       std::decay_t<T>, ComplexExpressionWithAdditionalCustomAtoms>::value,
                   bool>
  operator==(T const& other) const {
    return std::holds_alternative<
               ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, AdditionalCustomAtoms...>>(
               *this) &&
           (std::get<
                ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, AdditionalCustomAtoms...>>(
                *this) == other);
  }

  template <typename T>
  std::enable_if_t<boss::utilities::isVariantMember<T, AtomicExpressionWithAdditionalCustomAtoms<
                                                           AdditionalCustomAtoms...>>::value,
                   bool>
  operator==(T const& other) const {
    if(!std::holds_alternative<T>(*this)) {
      return false;
    }
    return std::get<T>(*this) == other;
  }
  template <typename T>
  std::enable_if_t<!std::is_same_v<T, ExpressionWithAdditionalCustomAtoms>, bool>
  operator!=(T const& other) const {
    return !(*this == other);
  }

  template <typename... Reason> ExpressionWithAdditionalCustomAtoms clone(Reason... reason) const {
    checkCloneWithoutReason(reason...);
    using ComplexExpression =
        ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, AdditionalCustomAtoms...>;
    return std::visit(
        boss::utilities::overload(
            [](auto const& val) -> ExpressionWithAdditionalCustomAtoms { return val; },
            [reason...](ComplexExpression const& val) -> ExpressionWithAdditionalCustomAtoms {
              return ComplexExpression(val.clone(reason...));
            }),
        (ExpressionWithAdditionalCustomAtoms::SuperType const&)*this);
  }

  friend ::std::ostream& operator<<(::std::ostream& out,
                                    ExpressionWithAdditionalCustomAtoms const& thing) {
    visit(
        boss::utilities::overload([&](::std::string const& value) { out << "\"" << value << "\""; },
                                  [&](bool value) { out << (value ? "True" : "False"); },
                                  [&](auto const& value) { out << value; }),
        thing);
    return out;
  }
};

template <typename... AdditionalCustomAtoms>
using ExpressionArgumentsWithAdditionalCustomAtoms =
    std::vector<ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>;

#ifdef ABLATION_NO_FAST_PATH
using atoms::SlowPathSpan;
template <typename... AdditionalCustomAtoms>
using ExpressionSlowPathSpanArgumentWithAdditionalCustomAtoms =
    std::variant<SlowPathSpan<bool>, SlowPathSpan<std::int32_t>, SlowPathSpan<std::int64_t>,
                 SlowPathSpan<std::double_t>, SlowPathSpan<std::string>, SlowPathSpan<Symbol>,
                 SlowPathSpan<AdditionalCustomAtoms>..., SlowPathSpan<bool const>,
                 SlowPathSpan<std::int32_t const>, SlowPathSpan<std::int64_t const>,
                 SlowPathSpan<std::double_t const>, SlowPathSpan<std::string const>,
                 SlowPathSpan<Symbol const>, SlowPathSpan<AdditionalCustomAtoms const>...>;
#endif // ABLATION_NO_FAST_PATH

template <typename... AdditionalCustomAtoms>
using ExpressionSpanArgumentWithAdditionalCustomAtoms =
    std::variant<Span<bool>, Span<std::int32_t>, Span<std::int64_t>, Span<std::double_t>,
                 Span<std::string>, Span<Symbol>, Span<AdditionalCustomAtoms>..., Span<bool const>,
                 Span<std::int32_t const>, Span<std::int64_t const>, Span<std::double_t const>,
                 Span<std::string const>, Span<Symbol const>, Span<AdditionalCustomAtoms const>...>;

template <typename... AdditionalCustomAtoms>
class ExpressionSpanArgumentsWithAdditionalCustomAtoms
    : public std::vector<
          ExpressionSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>> {
public:
  using std::vector<
      ExpressionSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::vector;

  // The Spans are not copyable anyway,
  // but we need to remove the copy constructors
  // so the compilers provide more useful errors at the calling point when trying to copy
  ExpressionSpanArgumentsWithAdditionalCustomAtoms(
      ExpressionSpanArgumentsWithAdditionalCustomAtoms const&) = delete;
  ExpressionSpanArgumentsWithAdditionalCustomAtoms&
  operator=(ExpressionSpanArgumentsWithAdditionalCustomAtoms const&) = delete;

  ExpressionSpanArgumentsWithAdditionalCustomAtoms() noexcept = default;
  ExpressionSpanArgumentsWithAdditionalCustomAtoms(
      ExpressionSpanArgumentsWithAdditionalCustomAtoms&&) noexcept = default;
  ExpressionSpanArgumentsWithAdditionalCustomAtoms&
  operator=(ExpressionSpanArgumentsWithAdditionalCustomAtoms&&) noexcept = default;
  ~ExpressionSpanArgumentsWithAdditionalCustomAtoms() = default;
};

#ifdef ABLATION_NO_FAST_PATH
using atoms::SlowPathSpan;
template <typename... AdditionalCustomAtoms>
using SlowPathSpanArgumentWithAdditionalCustomAtoms =
    std::variant<SlowPathSpan<bool>, SlowPathSpan<std::int32_t>, SlowPathSpan<std::int64_t>,
                 SlowPathSpan<std::double_t>, SlowPathSpan<std::string>, SlowPathSpan<Symbol>,
                 SlowPathSpan<AdditionalCustomAtoms>..., SlowPathSpan<bool const>,
                 SlowPathSpan<std::int32_t const>, SlowPathSpan<std::int64_t const>,
                 SlowPathSpan<std::double_t const>, SlowPathSpan<std::string const>,
                 SlowPathSpan<Symbol const>, SlowPathSpan<AdditionalCustomAtoms const>...>;
template <typename StaticArgumentsTuple, typename... AdditionalCustomAtoms>
class SlowPathSpanArgumentsWithAdditionalCustomAtoms
    : public std::vector<SlowPathSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>> {
public:
  SlowPathSpanArgumentsWithAdditionalCustomAtoms() noexcept : dummyExpr(Symbol{""}, {}) {}
  SlowPathSpanArgumentsWithAdditionalCustomAtoms( // NOLINT(bugprone-exception-escape)
      ExpressionSpanArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>&&
          baseSpans) noexcept
      : dummyExpr(
            new ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, AdditionalCustomAtoms...>(
                Symbol{""}, {}, {}, std::move(baseSpans))) {
    size_t beginIndex = 0; // std::tuple_size_v<StaticArgumentsTuple>;
    for(auto const& baseSpan : dummyExpr->getSpanArguments()) {
      std::visit(
          [this, &beginIndex](auto const& typedSpan) {
            using SpanType = std::remove_reference_t<decltype(typedSpan)>;
            using T = typename SpanType::element_type;
            auto size = typedSpan.size();
            if constexpr(std::is_constructible_v<SlowPathSpan<T>, decltype(dummyExpr), size_t,
                                                 decltype(size), decltype(typedSpan.begin())>) {
              std::vector<SlowPathSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::
                  emplace_back(SlowPathSpan<T>{dummyExpr, beginIndex, size, typedSpan.begin()});
            } else {
              std::vector<SlowPathSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::
                  emplace_back(SlowPathSpan<T>{dummyExpr, beginIndex, size, nullptr});
            }
            beginIndex += size;
          },
          baseSpan);
    }
  }

  // auto begin() { return dummyExpr.getSpanArguments().begin(); }
  // auto end() { return dummyExpr.getSpanArguments().end(); }

  // auto begin() const { return dummyExpr.getSpanArguments().begin(); }
  // auto end() const { return dummyExpr.getSpanArguments().end(); }

  // auto size() const { return dummyExpr.getSpanArguments().size(); }

  using std::vector<
      SlowPathSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::emplace_back;

  void emplace_back(
      ExpressionSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>&& baseSpan) {
    std::visit(
        [this](auto&& typedSpan) { emplace_back(std::forward<decltype(typedSpan)>(typedSpan)); },
        std::move(baseSpan));
  }

  template <typename T> void emplace_back(Span<T>&& span) {
    auto beginIndex = dummyExpr->getArguments().size();
    auto size = span.size();
    auto ptr = span.begin();
    std::vector<SlowPathSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::
        emplace_back(SlowPathSpan<T>{dummyExpr, beginIndex, size, ptr});
    dummyExpr->getSpanArguments().emplace_back(std::move(span));
  }

  explicit
  operator ExpressionSpanArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>() && {
    auto [_unused1, _unused2, _unused3, spans] = std::move(*dummyExpr).fastPathDecompose();
    std::vector<SlowPathSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::clear();
    return std::move(spans);
  }

  SlowPathSpanArgumentsWithAdditionalCustomAtoms(
      SlowPathSpanArgumentsWithAdditionalCustomAtoms const&) = delete;
  SlowPathSpanArgumentsWithAdditionalCustomAtoms&
  operator=(SlowPathSpanArgumentsWithAdditionalCustomAtoms const&) = delete;

  SlowPathSpanArgumentsWithAdditionalCustomAtoms(
      SlowPathSpanArgumentsWithAdditionalCustomAtoms&&) noexcept = default;
  SlowPathSpanArgumentsWithAdditionalCustomAtoms&
  operator=(SlowPathSpanArgumentsWithAdditionalCustomAtoms&&) noexcept = default;

  virtual ~SlowPathSpanArgumentsWithAdditionalCustomAtoms() = default;

private:
  std::shared_ptr<
      ComplexExpressionWithAdditionalCustomAtoms<std::tuple<>, AdditionalCustomAtoms...>>
      dummyExpr;
};
#endif // ABLATION_NO_FAST_PATH

/**
 * MovableReferenceWrapper is a re-implementation of std::reference_wrapper
 * but which allows moving the stored reference with 'operator T&&() &&' and 'get() &&'.
 * It is used for moving arguments from complex expressions,
 * e.g. in 'ComplexExpressionWithAdditionalCustomAtoms::getArgument(size_t) &&'.
 */
template <class T> class MovableReferenceWrapper {
public:
  typedef T type;

  explicit MovableReferenceWrapper(std::reference_wrapper<T>&& ref) {
    _ptr = std::addressof(ref.get());
  }

  MovableReferenceWrapper(MovableReferenceWrapper const&) noexcept = default;
  MovableReferenceWrapper& operator=(MovableReferenceWrapper const&) noexcept = default;
  MovableReferenceWrapper(MovableReferenceWrapper&&) noexcept = default;
  MovableReferenceWrapper& operator=(MovableReferenceWrapper&&) noexcept = default;
  ~MovableReferenceWrapper() = default;

  constexpr operator T&() const& { return *_ptr; } // NOLINT(hicpp-explicit-conversions)
  constexpr T& get() const& { return *_ptr; }

  template <typename T2>
  constexpr std::enable_if_t<boss::utilities::is_comparable<T, T2>::value, bool>
  operator==(MovableReferenceWrapper<T2> const other) const {
    return *_ptr == *other._ptr;
  }

  template <typename T2>
  constexpr std::enable_if_t<boss::utilities::is_comparable<T, T2>::value, bool>
  operator==(T2 const other) const {
    return *_ptr == other;
  }

  constexpr operator T&&() && { return std::move(*_ptr); } // NOLINT(hicpp-explicit-conversions)
  constexpr T get() && { return std::move(*_ptr); }

private:
  T* _ptr;

  template <typename T2> friend class MovableReferenceWrapper;
};

template <typename T, typename T2>
constexpr std::enable_if_t<boss::utilities::is_comparable<T2, T>::value, bool>
operator==(T const& left, MovableReferenceWrapper<T2> other) {
  return other == left;
}

template <bool ConstWrappee = false, typename... AdditionalCustomAtoms> class ArgumentWrapper;
template <typename... AdditionalCustomAtoms>
using ArgumentWrappeeType = typename boss::utilities::variant_amend<
    typename boss::utilities::rewrap_variant_arguments<
        MovableReferenceWrapper,
        AtomicExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::type,
    std::vector<bool>::reference,
    MovableReferenceWrapper<ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>>::type;

template <typename... AdditionalCustomAtoms>
using ConstArgumentWrappeeType = typename boss::utilities::variant_amend<
    typename utilities::rewrap_variant_arguments<
        MovableReferenceWrapper,
        typename utilities::make_variant_members_const<
            AtomicExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::type>::type,
    std::vector<bool>::const_reference,
    MovableReferenceWrapper<ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> const>>::
    type;

template <bool ConstWrappee, typename... AdditionalCustomAtoms> class ArgumentWrapper {
public:
  using WrappeeType =
      std::conditional_t<ConstWrappee, ConstArgumentWrappeeType<AdditionalCustomAtoms...>,
                         ArgumentWrappeeType<AdditionalCustomAtoms...>>;

private:
  WrappeeType argument;

public:
  WrappeeType& getArgument() & { return argument; };
  WrappeeType getArgument() && { return std::move(argument); };
  WrappeeType const& getArgument() const& { return argument; };

  operator // NOLINT(hicpp-explicit-conversions)
      ArgumentWrapper<true, AdditionalCustomAtoms...>() const {
    return std::visit(
        [](auto&& argument) {
          return ArgumentWrapper<true, AdditionalCustomAtoms...>(argument.get());
        },
        argument);
  };

  template <typename T> ArgumentWrapper& operator=(T&& newValue) {
    std::get<MovableReferenceWrapper<T>>(argument).get() = std::forward<T>(newValue);
    return *this;
  }
  template <typename T> ArgumentWrapper& operator=(T const& newValue) {
    argument = newValue;
    return *this;
  }

  /**
   * Only allow (move-)conversion to Expressions if the wrapper is non-const
   */
  template <bool Enable = !ConstWrappee,
            typename = typename std::enable_if<Enable>::type>
  operator // NOLINT(hicpp-explicit-conversions)
      ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>() && {
    return std::move(std::visit(
        [](auto&& e) -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
          if constexpr(boss::utilities::isInstanceOfTemplate<std::decay_t<decltype(e)>,
                                                             MovableReferenceWrapper>::value) {
            return std::forward<decltype(e)>(e).get();
          } else if constexpr(::std::disjunction_v<
                                  ::std::is_same<::std::decay_t<decltype(e)>,
                                                 ::std::vector<bool>::reference>,
                                  ::std::is_same<::std::decay_t<decltype(e)>,
                                                 ::std::vector<bool>::const_reference>>) {
            return (bool)e;
          } else {
            return std::forward<decltype(e)>(e);
          }
        },
        std::move(argument)));
  }

  /**
   * ArgumentWrappers wrap statically typed references to atomic types or references to
   * dynamically typed boss expressions. The provide a unified (dynamically-typed, visitor-based)
   * interface to them these types.
   */
  template <typename T,
            typename = std::enable_if_t<std::conjunction<
                std::negation<boss::utilities::isVariantMember<MovableReferenceWrapper<const T>,
                                                               WrappeeType>>,
                boss::utilities::isVariantMember<MovableReferenceWrapper<T>, WrappeeType>>::value>>
  ArgumentWrapper(T& argument) // NOLINT(hicpp-explicit-conversions)
      : argument(MovableReferenceWrapper(std::ref(argument))) {}
  template <
      typename T,
      typename = std::enable_if_t<std::conjunction<
          std::negation<boss::utilities::isVariantMember<MovableReferenceWrapper<T>, WrappeeType>>,
          boss::utilities::isVariantMember<MovableReferenceWrapper<const T>,
                                           WrappeeType>>::value>>
  ArgumentWrapper(T const& argument) // NOLINT(hicpp-explicit-conversions)
      : argument(MovableReferenceWrapper(std::cref(argument))) {}

  template <typename T, typename = std::enable_if_t<
                            std::disjunction_v<std::is_same<T, std::vector<bool>::const_reference>,
                                               std::is_same<T, std::vector<bool>::reference>>>>
  ArgumentWrapper(T&& argument) // NOLINT(hicpp-explicit-conversions)
      : argument([&argument]() {
          if constexpr(ConstWrappee || std::is_same_v<T, std::vector<bool>::const_reference>) {
            return static_cast<std::vector<bool>::const_reference>(argument);
          } else {
            return static_cast<std::vector<bool>::reference>(argument);
          }
        }()) {}

  bool valueless_by_exception() const { return argument.valueless_by_exception(); }

  auto at(size_t i) {
    return std::visit(boss::utilities::overload([i](auto&& arg) { return arg.at(i); }));
  }

  template <typename... Reason> auto clone(Reason... reason) const {
    checkCloneWithoutReason(reason...);
    static auto unwrap = [reason...](auto const& b) {
      if constexpr(boss::utilities::isInstanceOfTemplate<
                       std::decay_t<decltype(b)>, ExpressionWithAdditionalCustomAtoms>::value) {
        return b.clone(reason...);
      } else {
        return ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>(b);
      }
    };
    return std::visit(
        boss::utilities::overload([reason...](auto const& a) -> ExpressionWithAdditionalCustomAtoms<
                                                                 AdditionalCustomAtoms...> {
          if constexpr(boss::utilities::isInstanceOfTemplate<
                           std::decay_t<decltype(a)>, ExpressionWithAdditionalCustomAtoms>::value) {
            return a.get().clone(reason...);
          }
          if constexpr(boss::utilities::isInstanceOfTemplate<std::decay_t<decltype(a)>,
                                                             MovableReferenceWrapper>::value) {
            return unwrap(a.get());
          } else {
            return ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>(a);
          }
        }),
        argument);
  }

  template <typename T> auto operator==(T const& other) const {
    return std::visit(
        [&other](auto const& thisArgument) {
          if constexpr(boss::utilities::is_comparable<T, decltype(thisArgument)>::value) {
            return other == thisArgument;
          } else {
            return false;
          }
        },
        getArgument());
  };

  template <typename T> auto operator!=(T const& other) const { return !(*this == other); }

  friend ::std::ostream& operator<<(::std::ostream& stream, ArgumentWrapper const& argument) {
    return visit(
        [&stream](auto&& val) -> auto& {
          if constexpr(::std::disjunction_v<::std::is_same<::std::decay_t<decltype(val)>,
                                                           ::std::vector<bool>::reference>,
                                            ::std::is_same<::std::decay_t<decltype(val)>,
                                                           ::std::vector<bool>::const_reference>>) {
            return stream << (bool)val;
          } else {
            return stream << val.get();
          }
        },
        argument.getArgument());
  }
};

template <typename Func, auto ConstWrappee, typename... AdditionalCustomAtoms>
decltype(auto) visit(Func&& func,
                     ArgumentWrapper<ConstWrappee, AdditionalCustomAtoms...> const& wrapper) {
  return visit(
      [&](auto&& unwrapped) {
        if constexpr(boss::utilities::isInstanceOfTemplate<::std::decay_t<decltype(unwrapped)>,
                                                           MovableReferenceWrapper>::value) {
          if constexpr(::std::is_same_v<
                           ::std::remove_cv_t<::std::remove_reference_t<decltype(unwrapped.get())>>,
                           ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
            return visit(::std::forward<Func>(func), unwrapped.get());
          } else {
            return ::std::forward<Func>(func)(unwrapped.get());
          }
        } else if constexpr(::std::is_same_v<
                                ::std::remove_cv_t<::std::remove_reference_t<decltype(unwrapped)>>,
                                ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
          return visit(::std::forward<Func>(func), unwrapped);
        } else {
          return ::std::forward<Func>(func)(unwrapped);
        }
      },
      wrapper.getArgument());
}

template <typename Func, auto ConstWrappee, typename... AdditionalCustomAtoms>
decltype(auto) visit(Func&& func,
                     ArgumentWrapper<ConstWrappee, AdditionalCustomAtoms...>&& wrapper) {
  return visit(
      [&](auto&& unwrapped) {
        if constexpr(boss::utilities::isInstanceOfTemplate<::std::decay_t<decltype(unwrapped)>,
                                                           MovableReferenceWrapper>::value) {
          if constexpr(::std::is_same_v<
                           ::std::remove_cv_t<::std::remove_reference_t<decltype(unwrapped.get())>>,
                           ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
            return visit(::std::forward<Func>(func), unwrapped.get());
          } else {
            return ::std::forward<Func>(func)(unwrapped.get());
          }
        } else if constexpr(::std::is_same_v<
                                ::std::remove_cv_t<::std::remove_reference_t<decltype(unwrapped)>>,
                                ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
          return visit(::std::forward<Func>(func), unwrapped);
        } else {
          return ::std::forward<Func>(func)(unwrapped);
        }
      },
      wrapper.getArgument());
}

namespace utilities {
/**
 * utility template for use in constexpr contexts
 */
template <typename...> struct isConstArgumentWrapperType : public std::false_type {};
template <typename... T>
struct isConstArgumentWrapperType<ArgumentWrapper<true, T...>> : public std::true_type {};
template <typename... T>
inline constexpr bool isConstArgumentWrapper = isConstArgumentWrapperType<T...>::value;
} // namespace utilities

template <typename StaticArgumentsContainer, bool IsConstWrapper = false,
          typename... AdditionalAtoms>
class ExpressionArgumentsWithAdditionalCustomAtomsWrapper {
  std::conditional_t<IsConstWrapper, StaticArgumentsContainer const, StaticArgumentsContainer>&
      staticArguments;
  using DynamicArgumentsContainer =
      std::conditional_t<IsConstWrapper,
                         ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...> const,
                         ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...>>;
  DynamicArgumentsContainer& arguments;
  using SpanArgumentsContainer =
      std::conditional_t<IsConstWrapper,
                         ExpressionSpanArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...> const,
                         ExpressionSpanArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...>>;
  SpanArgumentsContainer& spanArguments;

public:
  ExpressionArgumentsWithAdditionalCustomAtomsWrapper(
      std::conditional_t<IsConstWrapper, StaticArgumentsContainer const, StaticArgumentsContainer>&
          staticArguments,
      DynamicArgumentsContainer& arguments, SpanArgumentsContainer& spanArguments)
      : staticArguments(staticArguments), arguments(arguments), spanArguments(spanArguments) {}

  size_t size() const {
    return std::tuple_size_v<StaticArgumentsContainer> + arguments.size() +
           std::accumulate(
               spanArguments.begin(), spanArguments.end(), 0, [](auto soFar, auto& thisOne) {
                 return soFar + std::visit([](auto&& thisOne) { return thisOne.size(); }, thisOne);
               });
  }
  bool empty() const { return size() == 0; }

  template <bool IsConstIterator> struct Iterator {
    using iterator_category = std::random_access_iterator_tag;
    using difference_type = long;
    using reference = ArgumentWrapper<IsConstIterator, AdditionalAtoms...>;
    using value_type = typename ArgumentWrapper<IsConstIterator, AdditionalAtoms...>::WrappeeType;
    using pointer = typename ArgumentWrapper<IsConstIterator, AdditionalAtoms...>::WrappeeType;

    std::conditional_t<IsConstIterator, ExpressionArgumentsWithAdditionalCustomAtomsWrapper const,
                       ExpressionArgumentsWithAdditionalCustomAtomsWrapper>
        container;
    size_t i;
    Iterator next() const {
      auto result = *this;
      result++;
      return result;
    }
    Iterator operator+(int i) const {
      auto result = *this;
      result.i += i;
      return result;
    }
    Iterator& operator++() {
      i++;
      return *this;
    }
    Iterator& operator--() {
      i--;
      return *this;
    }
    Iterator& operator+=(difference_type n) {
      i += n;
      return *this;
    }
    Iterator operator++(int) {
      auto before = *this;
      ++*this;
      return before;
    }
    Iterator operator--(int) {
      auto before = *this;
      --*this;
      return before;
    }
    std::ptrdiff_t operator-(Iterator const& other) const { return i - other.i; }

    ArgumentWrapper<IsConstIterator, AdditionalAtoms...> operator*() const {
      return container.at(i);
    }
    bool operator==(Iterator const& other) const { return i == other.i; }
    bool operator!=(Iterator const& other) const { return i != other.i; }
    bool operator<(Iterator const& other) const { return i < other.i; }
    bool operator>(Iterator const& other) const { return i > other.i; }

    // assignment operator required by some implementations of std::transform (e.g. on msvc)
    Iterator& operator=(Iterator&& other) noexcept {
      if(&other == this) {
        return *this;
      }
      i = other.i;
      static_assert(std::is_trivially_destructible_v<decltype(container)>);
      new(&container) decltype(container)(other.container.staticArguments,
                                          other.container.arguments, other.container.spanArguments);
      return *this;
    }
    Iterator& operator=(Iterator const& other) {
      if(&other == this) {
        return *this;
      }
      i = other.i;
      static_assert(std::is_trivially_destructible_v<decltype(container)>);
      new(&container) decltype(container)(other.container.staticArguments,
                                          other.container.arguments, other.container.spanArguments);
      return *this;
    }
    Iterator(Iterator const& other) = default;
    Iterator(Iterator&& other) noexcept = default;
    Iterator(std::conditional_t<IsConstIterator,
                                ExpressionArgumentsWithAdditionalCustomAtomsWrapper const,
                                ExpressionArgumentsWithAdditionalCustomAtomsWrapper>
                 container,
             size_t i)
        : container(container), i(i) {}
    ~Iterator() = default;
  };

  Iterator<IsConstWrapper> begin() const { return {*this, 0}; }

  Iterator<IsConstWrapper> end() const { return {*this, size()}; }

  template <size_t... I>
  constexpr ArgumentWrapper<IsConstWrapper, AdditionalAtoms...>
  getStaticArgument(size_t i, std::index_sequence<I...> /*unused*/) const {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    return std::move(std::array<ArgumentWrapper<IsConstWrapper, AdditionalAtoms...>, sizeof...(I)>{
        std::get<I>(staticArguments)...}[i]);
  }

  template <size_t... I>
  constexpr ArgumentWrapper<IsConstWrapper, AdditionalAtoms...> getStaticArgument(size_t i) const {
    return getStaticArgument(
        i, std::make_index_sequence<std::tuple_size_v<StaticArgumentsContainer>>());
  }

  ArgumentWrapper<IsConstWrapper, AdditionalAtoms...> front() const { return at(0); }

  ArgumentWrapper<IsConstWrapper, AdditionalAtoms...> operator[](size_t i) const {
    if constexpr(std::tuple_size_v < StaticArgumentsContainer >> 0) {
      if(i < std::tuple_size_v<StaticArgumentsContainer>) {
        return getStaticArgument(i);
      }
    } else if((i - std::tuple_size_v<StaticArgumentsContainer>) < arguments.size()) {
      return arguments[i - std::tuple_size_v<StaticArgumentsContainer>];
    } else {
      auto argumentPrefixScan = std::tuple_size_v<StaticArgumentsContainer> + arguments.size();
      for(auto& spanArgument : spanArguments) {
        if(i >= argumentPrefixScan &&
           i < argumentPrefixScan +
                   std::visit([](auto&& spanArgument) { return spanArgument.size(); },
                              spanArgument)) {
          return std::visit(
              [&](auto&& spanArgument) -> ArgumentWrapper<IsConstWrapper, AdditionalAtoms...> {
                if constexpr((std::is_same_v<std::decay_t<decltype(spanArgument.at(0))>,
                                             std::vector<bool>::const_reference> &&
                              !IsConstWrapper) ||
                             ((std::is_const_v<std::remove_reference_t<decltype(spanArgument)>> ||
                               std::is_const_v<std::remove_reference_t<decltype(spanArgument.at(
                                   0))>>)&&!IsConstWrapper)) {
                  throw std::runtime_error("cannot convert const span to non-const argument");
                } else {
                  return spanArgument[i - argumentPrefixScan];
                }
              },
              spanArgument);
        }
        argumentPrefixScan +=
            std::visit([](auto&& spanArgument) { return spanArgument.size(); }, spanArgument);
      }
    }
#if defined(_MSC_VER)
    __assume(0);
#else
    __builtin_unreachable();
#endif
  }

  ArgumentWrapper<IsConstWrapper, AdditionalAtoms...> at(size_t i) const {
    if constexpr((std::tuple_size_v<StaticArgumentsContainer>) > 0) {
      if(i < std::tuple_size_v<StaticArgumentsContainer>) {
        return getStaticArgument(i);
      }
    }
    if((i - std::tuple_size_v<StaticArgumentsContainer>) < arguments.size()) {
      return arguments.at(i - std::tuple_size_v<StaticArgumentsContainer>);
    }
    auto argumentPrefixScan = std::tuple_size_v<StaticArgumentsContainer> + arguments.size();
    for(auto& spanArgument : spanArguments) {
      if(i >= argumentPrefixScan &&
         i < argumentPrefixScan + std::visit([](auto& t) { return t.size(); }, spanArgument)) {
        return std::visit(
            [&](auto&& spanArgument) -> ArgumentWrapper<IsConstWrapper, AdditionalAtoms...> {
              if constexpr((!IsConstWrapper &&
                            std::is_same_v<std::decay_t<decltype(spanArgument.at(0))>,
                                           std::vector<bool>::const_reference>) ||
                           ((std::is_const_v<std::remove_reference_t<decltype(spanArgument)>> ||
                             std::is_const_v<std::remove_reference_t<decltype(spanArgument.at(
                                 0))>>)&&!IsConstWrapper)) {
                throw std::runtime_error("cannot convert const span to non-const argument");
              } else if constexpr(

                  std::is_same_v<std::decay_t<decltype(spanArgument.at(0))>,
                                 std::vector<bool>::reference> ||
                  std::is_same_v<std::decay_t<decltype(spanArgument.at(0))>,
                                 std::vector<bool>::const_reference>) {
                if constexpr(IsConstWrapper ||
                             std::is_same_v<std::decay_t<decltype(spanArgument.at(0))>,
                                            std::vector<bool>::const_reference>) {
                  return std::vector<bool>::const_reference(
                      spanArgument.at(i - argumentPrefixScan));
                } else {
                  return std::vector<bool>::reference(spanArgument.at(i - argumentPrefixScan));
                }
              } else {
                return spanArgument.at(i - argumentPrefixScan);
              }
            },
            spanArgument);
      }
      argumentPrefixScan +=
          std::visit([](auto&& spanArgument) { return spanArgument.size(); }, spanArgument);
    }
    throw std::out_of_range("Expression has no argument with index " + std::to_string(i));
  }

  operator // NOLINT(hicpp-explicit-conversions)
      ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...>() const& {
    ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...> result;
    result.reserve(this->size());
    std::transform(std::begin(*this), std::end(*this), back_inserter(result),
                   [](auto&& wrapper) { return wrapper.clone(); });
    return std::move(result);
  }

  operator // NOLINT(hicpp-explicit-conversions)
      ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...>() & {
    ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...> result;
    result.reserve(this->size());
    std::transform(std::begin(*this), std::end(*this), back_inserter(result), [](auto&& wrapper) {
      return wrapper.clone(CloneReason::IMPLICIT_CONVERSION_WITH_GET_ARGUMENTS);
    });
    return std::move(result);
  }

  /**
   * Only allow (move-)conversion to ExpressionArguments if the wrapper is non-const
   * otherwise apply the (copy-)conversion (same as for l-reference)
   */
  operator // NOLINT(hicpp-explicit-conversions)
      ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...>() && {
    if constexpr(!IsConstWrapper && (std::tuple_size_v<StaticArgumentsContainer>) == 0) {
      if(spanArguments.empty()) {
        // avoid any copying if there are only ExpressionArguments
        return std::move(arguments);
      }
    }
    ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalAtoms...> result;
    result.reserve(this->size());
    std::transform(std::make_move_iterator(std::begin(*this)),
                   std::make_move_iterator(std::end(*this)), back_inserter(result),
                   [](auto&& wrapper) -> ExpressionWithAdditionalCustomAtoms<AdditionalAtoms...> {
                     if constexpr(!IsConstWrapper &&
                                  !std::is_lvalue_reference_v<decltype(wrapper)>) {
                       return std::forward<decltype(wrapper)>(wrapper);
                     } else {
                       return wrapper.clone(CloneReason::IMPLICIT_CONVERSION_WITH_GET_ARGUMENTS);
                     }
                   });
    return std::move(result);
  }

  template <typename T> void emplace_back(T t) {
    assert(spanArguments.size() == 0);
    arguments.emplace_back(t);
  }
};

template <typename StaticArgumentsTuple, typename... AdditionalCustomAtoms>
class ComplexExpressionWithAdditionalCustomAtoms;

template <typename... AdditionalCustomAtoms> class ExtensibleExpressionSystem {
public:
  using AtomicExpression = AtomicExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;
  template <typename... StaticArgumentTypes>
  using ComplexExpressionWithStaticArguments =
      ComplexExpressionWithAdditionalCustomAtoms<std::tuple<StaticArgumentTypes...>,
                                                 AdditionalCustomAtoms...>;
  using ComplexExpression = ComplexExpressionWithStaticArguments<>;
  using Expression = ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;
  using ExpressionArguments =
      ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;
  using ExpressionSpanArguments =
      ExpressionSpanArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;
  using ExpressionSpanArgument =
      ExpressionSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;
#ifdef ABLATION_NO_FAST_PATH
  template <typename StaticArgumentsTuple>
  using SlowPathSpanArguments =
      SlowPathSpanArgumentsWithAdditionalCustomAtoms<StaticArgumentsTuple,
                                                     AdditionalCustomAtoms...>;
  using SlowPathSpanArgument =
      SlowPathSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;
#endif // ABLATION_NO_FAST_PATH
};

template <typename StaticArgumentsTuple, typename... AdditionalCustomAtoms>
class ComplexExpressionWithAdditionalCustomAtoms {
public:
  using ExpressionArguments =
      ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;
  using ExpressionSpanArguments =
      ExpressionSpanArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>;

private:
  Symbol head;
  StaticArgumentsTuple staticArguments{};
  ExpressionArguments arguments{};
  ExpressionSpanArguments spanArguments{};

public:
  template <size_t... I>
  static StaticArgumentsTuple
  convertToTuple(ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>& arguments,
                 std::index_sequence<I...> /*unused*/) {
    return {(std::get<
             std::remove_reference_t<typename std::tuple_element<I, StaticArgumentsTuple>::type>>(
        arguments.at(I)))...};
  }

  template <typename T>
  void cloneIfNecessary(
      ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>& result,
      ComplexExpressionWithAdditionalCustomAtoms<T, AdditionalCustomAtoms...> const& e) const {
    result.push_back(e.clone());
  }

  template <typename T,
            typename = std::enable_if_t<boss::utilities::isVariantMember<
                T, AtomicExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>::value>>
  void
  cloneIfNecessary(ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>& result,
                   T e) const {
    result.emplace_back(e);
  }

  /**
   * This will move the components out of the expression and leave the expression empty
   * (don't move individual members out of the expresion !!!)
   */
#ifdef ABLATION_NO_FAST_PATH
  using SlowSpanArguments =
      SlowPathSpanArgumentsWithAdditionalCustomAtoms<StaticArgumentsTuple,
                                                     AdditionalCustomAtoms...>;
  std::tuple<Symbol, StaticArgumentsTuple, ExpressionArguments, SlowSpanArguments> decompose() && {
    return {std::move(head), std::move(staticArguments), std::move(arguments),
            SlowSpanArguments(std::move(spanArguments))};
  }

  std::tuple<Symbol, StaticArgumentsTuple, ExpressionArguments, ExpressionSpanArguments>
  fastPathDecompose() && {
    return {std::move(head), std::move(staticArguments), std::move(arguments),
            std::move(spanArguments)};
  }
#else
  std::tuple<Symbol, StaticArgumentsTuple, ExpressionArguments, ExpressionSpanArguments>
  decompose() && {
    return {std::move(head), std::move(staticArguments), std::move(arguments),
            std::move(spanArguments)};
  }
#endif // ABLATION_NO_FAST_PATH

  template <size_t... I>
  ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...>
  convertStaticToDynamicArguments(std::index_sequence<I...> /*unused*/) const {
    ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...> result;
    result.reserve(arguments.size() + sizeof...(I));
    (cloneIfNecessary(result, std::get<I>(staticArguments)), ...);
    std::for_each(arguments.begin(), arguments.end(), [this, &result](auto&& e) {
      std::visit([this, &result](auto&& e) { cloneIfNecessary(result, e); }, e);
    });
    return result;
  }

#ifdef ABLATION_NO_FAST_PATH
  template <typename OtherStaticArgumentsTuple>
  ComplexExpressionWithAdditionalCustomAtoms(
      Symbol const& head, StaticArgumentsTuple&& staticArguments, ExpressionArguments&& arguments,
      SlowPathSpanArgumentsWithAdditionalCustomAtoms<OtherStaticArgumentsTuple,
                                                     AdditionalCustomAtoms...>&& spanArguments)
      : ComplexExpressionWithAdditionalCustomAtoms(
            head, std::move(staticArguments), std::move(arguments),
            (ExpressionSpanArguments)std::move(spanArguments)) {}

  template <typename OtherStaticArgumentsTuple>
  ComplexExpressionWithAdditionalCustomAtoms(
      Symbol&& head, StaticArgumentsTuple&& staticArguments, ExpressionArguments&& arguments,
      SlowPathSpanArgumentsWithAdditionalCustomAtoms<OtherStaticArgumentsTuple,
                                                     AdditionalCustomAtoms...>&& spanArguments)
      : ComplexExpressionWithAdditionalCustomAtoms(
            std::move(head), std::move(staticArguments), std::move(arguments),
            (ExpressionSpanArguments)std::move(spanArguments)) {}
#endif // ABLATION_NO_FAST_PATH

  ComplexExpressionWithAdditionalCustomAtoms(Symbol const& head,
                                             StaticArgumentsTuple&& staticArguments,
                                             ExpressionArguments&& arguments = {},
                                             ExpressionSpanArguments&& spanArguments = {})
      : head(head), staticArguments(std::move(staticArguments)), arguments(std::move(arguments)),
        spanArguments(std::move(spanArguments)) {}

  ComplexExpressionWithAdditionalCustomAtoms(Symbol&& head, StaticArgumentsTuple&& staticArguments,
                                             ExpressionArguments&& arguments = {},
                                             ExpressionSpanArguments&& spanArguments = {})
      : head(std::move(head)), staticArguments(std::move(staticArguments)),
        arguments(std::move(arguments)), spanArguments(std::move(spanArguments)) {}

  template <typename = std::enable_if<std::tuple_size<StaticArgumentsTuple>::value == 0>>
  explicit ComplexExpressionWithAdditionalCustomAtoms(Symbol const& head,
                                                      ExpressionArguments&& arguments)
      : ComplexExpressionWithAdditionalCustomAtoms(
            head,
            convertToTuple(
                arguments,
                std::make_index_sequence<std::tuple_size<StaticArgumentsTuple>::value>()),
            {std::move_iterator(
                 next(begin(arguments), std::tuple_size<StaticArgumentsTuple>::value)),
             std::move_iterator(end(arguments))}){};

  template <typename = std::enable_if<std::tuple_size<StaticArgumentsTuple>::value == 0>>
  explicit ComplexExpressionWithAdditionalCustomAtoms(Symbol&& head,
                                                      ExpressionArguments&& arguments)
      : ComplexExpressionWithAdditionalCustomAtoms(
            std::move(head),
            convertToTuple(
                arguments,
                std::make_index_sequence<std::tuple_size<StaticArgumentsTuple>::value>()),
            {std::move_iterator(
                 next(begin(arguments), std::tuple_size<StaticArgumentsTuple>::value)),
             std::move_iterator(end(arguments))}){};

  operator ComplexExpressionWithAdditionalCustomAtoms< // NOLINT(hicpp-explicit-conversions)
      std::tuple<>, AdditionalCustomAtoms...>() const {
    return std::move(
        ComplexExpressionWithAdditionalCustomAtoms< // NOLINT(hicpp-explicit-conversions)
            std::tuple<>, AdditionalCustomAtoms...>(
            head, convertStaticToDynamicArguments(
                      std::make_index_sequence<std::tuple_size<StaticArgumentsTuple>::value>())));
  }

  template <typename = std::enable_if<sizeof...(AdditionalCustomAtoms) != 0>, typename OtherTuple,
            typename... T>
  explicit ComplexExpressionWithAdditionalCustomAtoms(
      ComplexExpressionWithAdditionalCustomAtoms<OtherTuple, T...>&& other)
      : head(std::move(other).getHead()) {
#ifdef ABLATION_NO_FAST_PATH
    auto [_unused, otherStatics, otherDynamics, otherSpans] = std::move(other).fastPathDecompose();
#else
    auto [_unused, otherStatics, otherDynamics, otherSpans] = std::move(other).decompose();
#endif // ABLATION_NO_FAST_PATH
    arguments.reserve(std::tuple_size_v<OtherTuple> + otherDynamics.size());
    // move statics
    std::apply(
        [this](auto&&... staticArgs) { (arguments.emplace_back(std::move(staticArgs)), ...); },
        std::move(otherStatics));
    // move dynamics
    for(auto&& arg : otherDynamics) {
      visit(boss::utilities::overload(
                [this](ComplexExpressionWithAdditionalCustomAtoms<OtherTuple, T...>&& typedArg) {
                  arguments.emplace_back(
                      ComplexExpressionWithAdditionalCustomAtoms(std::move(typedArg)));
                },
                [this](auto&& typedArg) {
                  arguments.emplace_back(std::forward<decltype(typedArg)>(typedArg));
                }),
            std::move(arg));
    }
    spanArguments.reserve(otherSpans.size());
    for(auto&& span : otherSpans) {
      std::visit(
          [this](auto&& typedSpan) {
            spanArguments.emplace_back(std::forward<decltype(typedSpan)>(typedSpan));
          },
          std::move(span));
    }
  }

  ExpressionArgumentsWithAdditionalCustomAtomsWrapper<StaticArgumentsTuple, false,
                                                      AdditionalCustomAtoms...>
  getArguments() {
    return {staticArguments, arguments, spanArguments};
  }
  ExpressionArgumentsWithAdditionalCustomAtomsWrapper<StaticArgumentsTuple, true,
                                                      AdditionalCustomAtoms...>
  getArguments() const {
    return {staticArguments, arguments, spanArguments};
  }

  ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...> const&
  getDynamicArguments() const& {
    return arguments;
  };

  ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...> getDynamicArguments() && {
    return std::move(arguments);
  };

  auto const& getStaticArguments() const& { return staticArguments; }
  auto getStaticArguments() && { return std::move(staticArguments); }
  auto const& getSpanArguments() const& { return spanArguments; }
  auto getSpanArguments() && { return std::move(spanArguments); }

#ifdef ABLATION_NO_FAST_PATH
  auto& getSpanArguments() & { return spanArguments; }
#endif // ABLATION_NO_FAST_PATH

  ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> getArgument(size_t i) && {
    return visit(
        [](auto&& unwrapped) -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
          if constexpr(boss::utilities::isInstanceOfTemplate<::std::decay_t<decltype(unwrapped)>,
                                                             MovableReferenceWrapper>::value) {
            if constexpr(::std::is_same_v<
                             ::std::remove_cv_t<
                                 ::std::remove_reference_t<decltype(unwrapped.get())>>,
                             ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
              return visit(
                  [](auto&& arg) -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
                    return ::std::forward<decltype(arg)>(arg);
                  },
                  ::std::forward<decltype(unwrapped)>(unwrapped).get());
            } else {
              return ::std::forward<decltype(unwrapped)>(unwrapped).get();
            }
          } else if constexpr(::std::is_same_v<
                                  ::std::remove_cv_t<
                                      ::std::remove_reference_t<decltype(unwrapped)>>,
                                  ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
            return visit(
                [](auto&& arg) -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
                  return ::std::forward<decltype(arg)>(arg);
                },
                ::std::forward<decltype(unwrapped)>(unwrapped));
          } else {
            return ::std::forward<decltype(unwrapped)>(unwrapped);
          }
        },
        std::forward<decltype(getArguments().at(i).getArgument())>(
            getArguments().at(i).getArgument()));
  }

  template <typename... Reason>
  ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>
  cloneArgument(size_t i, Reason... reason) const {
    checkCloneWithoutReason(reason...);
    return visit(
        [reason...](auto const& unwrapped)
            -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
          if constexpr(boss::utilities::isInstanceOfTemplate<::std::decay_t<decltype(unwrapped)>,
                                                             MovableReferenceWrapper>::value) {
            if constexpr(::std::is_same_v<
                             ::std::remove_cv_t<
                                 ::std::remove_reference_t<decltype(unwrapped.get())>>,
                             ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
              return visit(
                  boss::utilities::overload(
                      [reason...](ComplexExpressionWithAdditionalCustomAtoms const& arg)
                          -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
                        return arg.clone(reason...);
                      },
                      [](auto const& arg)
                          -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
                        return arg;
                      }),
                  unwrapped.get());
            } else {
              return unwrapped.get();
            }
          } else if constexpr(::std::is_same_v<
                                  ::std::remove_cv_t<
                                      ::std::remove_reference_t<decltype(unwrapped)>>,
                                  ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>) {
            return visit(
                [reason...](auto const& arg)
                    -> ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
                  return arg.clone(reason...);
                },
                unwrapped);
          } else {
            return unwrapped;
          }
        },
        getArguments().at(i).getArgument());
  }

  Symbol const& getHead() const& { return head; };
  Symbol getHead() && { return std::move(head); };

  ~ComplexExpressionWithAdditionalCustomAtoms() = default;
  ComplexExpressionWithAdditionalCustomAtoms(
      ComplexExpressionWithAdditionalCustomAtoms&&) noexcept = default;
  ComplexExpressionWithAdditionalCustomAtoms&
  operator=(ComplexExpressionWithAdditionalCustomAtoms&&) noexcept = default;

  template <typename... StaticArgumentTypes>
  bool operator==(
      ComplexExpressionWithAdditionalCustomAtoms<std::tuple<StaticArgumentTypes...>,
                                                 AdditionalCustomAtoms...> const& other) const {
    if(getHead() != other.getHead() || getArguments().size() != other.getArguments().size()) {
      return false;
    }
    for(auto i = 0U; i < getArguments().size(); i++) {
      if(getArguments()[i] != other.getArguments()[i]) {
        return false;
      }
    }
    return true;
  }
  bool operator!=(ComplexExpressionWithAdditionalCustomAtoms const& other) const {
    return !(*this == other);
  }

  template <typename... Reason>
  ComplexExpressionWithAdditionalCustomAtoms clone(Reason... reason) const {
    checkCloneWithoutReason(reason...);
    ExpressionArgumentsWithAdditionalCustomAtoms<AdditionalCustomAtoms...> copiedArgs;
    typename generic::ExtensibleExpressionSystem<AdditionalCustomAtoms...>::ExpressionSpanArguments
        newSpanArguments;
    static_assert(std::tuple_size_v<decltype(staticArguments)> == 0);
    for(auto const& arg : getDynamicArguments()) {
      copiedArgs.emplace_back(arg.clone(reason...));
    }

    newSpanArguments.reserve(spanArguments.size());
    for(auto& it : spanArguments) {
      newSpanArguments.push_back(std::visit(
          [&](auto const& v)
              -> ExpressionSpanArgumentWithAdditionalCustomAtoms<AdditionalCustomAtoms...> {
            return v.clone(reason...);
          },
          it));
    }

    return ComplexExpressionWithAdditionalCustomAtoms(head, {}, std::move(copiedArgs),
                                                      std::move(newSpanArguments));
  }

  /**
   * a specialization for complex expressions is needed. Otherwise the complex
   * expression and all its arguments have to be copied to be converted to an
   * Expression
   */
  friend ::std::ostream& operator<<(::std::ostream& out,
                                    ComplexExpressionWithAdditionalCustomAtoms const& e) {
    out << e.getHead() << "[";
    if(!e.getArguments().empty()) {
      out << e.getArguments().front();
      for(auto it = ::std::next(e.getArguments().begin()); it != e.getArguments().end(); ++it) {
        out << "," << *it;
      }
    }
    out << "]";
    return out;
  }

  ComplexExpressionWithAdditionalCustomAtoms(ComplexExpressionWithAdditionalCustomAtoms const&) =
      delete;
  ComplexExpressionWithAdditionalCustomAtoms&
  operator=(ComplexExpressionWithAdditionalCustomAtoms const&) = delete;
};

template <typename T, typename... AdditionalCustomAtoms>
T const& get(generic::ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...> const& e) {
  try {
    return std::get<T>(e);
  } catch(std::bad_variant_access&) {
    throw ArgumentTypeMismatch<T>(e);
  }
}

template <typename T, typename... AdditionalCustomAtoms>
T& get(generic::ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>& e) {
  try {
    return std::get<T>(e);
  } catch(std::bad_variant_access&) {
    throw ArgumentTypeMismatch<T>(e);
  }
}

template <typename T, typename... AdditionalCustomAtoms>
T get(generic::ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>&& e) {
  try {
    return std::get<T>(std::move(e));
  } catch(std::bad_variant_access&) {
    throw ArgumentTypeMismatch<T>(e);
  }
}

template <typename T, auto ConstWrappee, typename... AdditionalCustomAtoms>
T& get(generic::ArgumentWrapper<ConstWrappee, AdditionalCustomAtoms...> const& wrapper) {
  try {
    return ::std::visit(
        [](auto& argument) -> T& {
          if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>,
                                        MovableReferenceWrapper<T>>) {
            return argument.get();
          } else if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>,
                                               ::std::vector<bool>::reference>) {
            if constexpr(::std::is_same_v<::std::decay_t<T>, ::std::vector<bool>::reference>) {

              return argument;
            }
            throw ::std::bad_variant_access();
          } else if constexpr(boss::utilities::isInstanceOfTemplate<
                                  ::std::decay_t<decltype(argument)>,
                                  MovableReferenceWrapper>::value) {
            if constexpr(boss::utilities::isInstanceOfTemplate<
                             ::std::decay_t<decltype(argument.get())>,
                             ExpressionWithAdditionalCustomAtoms>::value) {
              return ::std::get<T>(argument.get());
            }
            throw ::std::bad_variant_access();
          } else {
            throw ::std::bad_variant_access();
          }
        },
        wrapper.getArgument());
  } catch(std::bad_variant_access&) {
    throw ArgumentTypeMismatch<T>(wrapper);
  }
}

template <typename T, typename... AdditionalCustomAtoms>
T const& get(ArgumentWrapper<true, AdditionalCustomAtoms...> const& wrapper) {
  try {
    return ::std::visit(
        [](auto const& wrappee) -> T const& {
          if constexpr(boss::utilities::isInstanceOfTemplate<::std::decay_t<decltype(wrappee)>,
                                                             MovableReferenceWrapper>::value) {
            if constexpr(::std::is_same_v<typename ::std::decay_t<decltype(wrappee)>::type, T>) {
              return wrappee.get();
            } else if constexpr(boss::utilities::isInstanceOfTemplate<
                                    ::std::decay_t<decltype(wrappee.get())>,
                                    ExpressionWithAdditionalCustomAtoms>::value) {
              return std::get<T>(wrappee.get());
            }
            throw ::std::bad_variant_access();
          } else if constexpr(::std::is_same_v<::std::decay_t<decltype(wrappee)>,
                                               ::std::vector<bool>::reference> ||
                              ::std::is_same_v<::std::decay_t<decltype(wrappee)>,
                                               ::std::vector<bool>::const_reference>) {
            if constexpr(::std::is_same_v<bool, T>) {
              return wrappee;
            }
            throw ::std::bad_variant_access();
          } else {
            return get<T>(wrappee);
          }
        },
        wrapper.getArgument());
  } catch(std::bad_variant_access&) {
    throw ArgumentTypeMismatch<T>(wrapper);
  }
}

template <size_t I, bool ConstWrappee, typename... AdditionalCustomAtoms>
constexpr ::std::variant_alternative_t<I, ArgumentWrappeeType<AdditionalCustomAtoms...>>&
get(ArgumentWrapper<ConstWrappee, AdditionalCustomAtoms...> const& wrapper) noexcept {
  return ::std::get<I>(wrapper.getArgument());
};

template <typename T, auto ConstWrappee, typename... AdditionalCustomAtoms>
bool holds_alternative(
    generic::ArgumentWrapper<ConstWrappee, AdditionalCustomAtoms...> const& wrapper) {
  return ::std::visit(
      [](auto& argument) {
        if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>,
                                      MovableReferenceWrapper<T>>) {
          return true;
        } else if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>,
                                             ::std::vector<bool>::reference>) {
          if constexpr(::std::is_same_v<::std::decay_t<T>, ::std::vector<bool>::reference>) {

            return true;
          }
        } else if constexpr(boss::utilities::isInstanceOfTemplate<
                                ::std::decay_t<decltype(argument)>,
                                MovableReferenceWrapper>::value) {
          if constexpr(boss::utilities::isInstanceOfTemplate<
                           ::std::decay_t<decltype(argument.get())>,
                           ExpressionWithAdditionalCustomAtoms>::value) {
            return ::std::holds_alternative<T>(argument.get());
          }
        }
        return false;
      },
      wrapper.getArgument());
}

template <typename T, auto ConstWrappee = false, typename... AdditionalCustomAtoms>
decltype(auto) get_if(ArgumentWrapper<ConstWrappee, AdditionalCustomAtoms...> const* wrapper) {
  return ::std::visit(
      [](auto& argument) -> std::conditional_t<ConstWrappee, T const*, T*> {
        if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>,
                                      MovableReferenceWrapper<T>>) {
          return &argument.get();
        } else if constexpr(::std::is_same_v<::std::decay_t<decltype(argument)>,
                                             ::std::vector<bool>::reference>) {
          if constexpr(::std::is_same_v<::std::decay_t<T>, ::std::vector<bool>::reference>) {

            return &argument;
          }
          return nullptr;
        } else if constexpr(boss::utilities::isInstanceOfTemplate<
                                ::std::decay_t<decltype(argument)>,
                                MovableReferenceWrapper>::value) {
          if constexpr(boss::utilities::isInstanceOfTemplate<
                           ::std::decay_t<decltype(argument.get())>,
                           ExpressionWithAdditionalCustomAtoms>::value) {
            return ::std::get_if<T>(&argument.get());
          }
          return nullptr;
        } else {
          return nullptr;
        }
      },
      wrapper->getArgument());
}

} // namespace generic
using DefaultExpressionSystem = generic::ExtensibleExpressionSystem<>;

using AtomicExpression = DefaultExpressionSystem::AtomicExpression;
template <typename... StaticArgumentTypes>
using ComplexExpressionWithStaticArguments =
    DefaultExpressionSystem::ComplexExpressionWithStaticArguments<StaticArgumentTypes...>;
using ComplexExpression = DefaultExpressionSystem::ComplexExpressionWithStaticArguments<>;
using Expression = DefaultExpressionSystem::Expression;
using ExpressionArguments = DefaultExpressionSystem::ExpressionArguments;
using ExpressionSpanArguments = DefaultExpressionSystem::ExpressionSpanArguments;
using ExpressionSpanArgument = DefaultExpressionSystem::ExpressionSpanArgument;
#ifdef ABLATION_NO_FAST_PATH
template <typename StaticArgumentsTuple>
using SlowPathSpanArguments = DefaultExpressionSystem::SlowPathSpanArguments<StaticArgumentsTuple>;
using SlowPathSpanArgument = DefaultExpressionSystem::SlowPathSpanArgument;
#endif // ABLATION_NO_FAST_PATH
} // namespace expressions

using expressions::ComplexExpression;
using expressions::ComplexExpressionWithStaticArguments;
using expressions::DefaultExpressionSystem;
using expressions::Expression;
using expressions::ExpressionArguments;
using expressions::Span; // NOLINT
using expressions::Symbol;
using expressions::generic::ExtensibleExpressionSystem; // NOLINT
using expressions::generic::get;                        // NOLINT
using expressions::generic::get_if;                     // NOLINT
using expressions::generic::holds_alternative;          // NOLINT
} // namespace boss

namespace std {

template <typename... AdditionalCustomAtoms>
struct variant_size<typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
    AdditionalCustomAtoms...>>
    : variant_size<typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
          AdditionalCustomAtoms...>::SuperType> {};

template <typename... AdditionalCustomAtoms>
struct variant_size<const typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
    AdditionalCustomAtoms...>>
    : variant_size<const typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
          AdditionalCustomAtoms...>::SuperType> {};

template <::std::size_t I, typename... AdditionalCustomAtoms>
struct variant_alternative<I, typename boss::expressions::generic::
                                  ExpressionWithAdditionalCustomAtoms<AdditionalCustomAtoms...>>
    : variant_alternative<I,
                          typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
                              AdditionalCustomAtoms...>::SuperType> {};
template <typename Func, typename... AdditionalCustomAtoms>
decltype(auto) visit(Func&& func,
                     typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
                         AdditionalCustomAtoms...>& e) {
  return visit(::std::forward<Func>(func),
               (typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
                   AdditionalCustomAtoms...>::SuperType&)e);
};
template <typename Func, typename... AdditionalCustomAtoms>
decltype(auto) visit(Func&& func,
                     typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
                         AdditionalCustomAtoms...> const& e) {
  return visit(::std::forward<Func>(func),
               (typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
                   AdditionalCustomAtoms...>::SuperType const&)e);
};
template <typename Func, typename... AdditionalCustomAtoms>
decltype(auto) visit(Func&& func,
                     typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
                         AdditionalCustomAtoms...>&& e) {
  return visit(::std::forward<Func>(func),
               (typename boss::expressions::generic::ExpressionWithAdditionalCustomAtoms<
                    AdditionalCustomAtoms...>::SuperType &&)::std::move(e));
};
template <> struct hash<boss::expressions::Symbol> {
  ::std::size_t operator()(boss::expressions::Symbol const& s) const noexcept {
    return ::std::hash<::std::string>{}(s.getName());
  }
};

#ifdef __clang__

#elif __GNUC__
namespace __detail {
namespace __variant {
template <typename... CustomAtoms>
struct _Extra_visit_slot_needed<
    ::std::__detail::__variant::__deduce_visit_result<void>,
    const boss::ExpressionWithAdditionalCustomAtoms<CustomAtoms...>&> // NOLINT
{
  template <typename> struct _Variant_never_valueless : false_type {}; // NOLINT
  static constexpr bool value = false;
};

template <typename... CustomAtoms>
struct _Extra_visit_slot_needed<
    ::std::__detail::__variant::__deduce_visit_result<void>,
    const boss::ExpressionWithAdditionalCustomAtoms<CustomAtoms...>> // NOLINT
{
  template <typename> struct _Variant_never_valueless : false_type {}; // NOLINT
  static constexpr bool value = false;
};

} // namespace __variant
} // namespace __detail
#endif

} // namespace std
