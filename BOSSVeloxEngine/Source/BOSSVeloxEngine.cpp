#include "BOSSVeloxEngine.hpp"

#include "BOSSCoreTmp.h"
#include "BridgeVelox.h"

#include "velox/exec/PlanNodeStats.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

#include <any>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

#include <ExpressionUtilities.hpp>
#include <cstring>
#include <regex>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#ifdef DebugInfo
#include <iostream>
#endif // DebugInfo

using std::get;
using std::is_invocable_v;
using std::move;
using std::to_string;
using std::unordered_map;
using std::string_literals::operator""s;

using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Expression;
using boss::Span;
using boss::Symbol;
using boss::expressions::generic::isComplexExpression;

namespace boss {
using SpanInputs =
    std::variant<std::vector<std::int32_t>, std::vector<std::int64_t>, std::vector<std::double_t>,
                 std::vector<std::string>, std::vector<Symbol>>;
} // namespace boss

namespace boss::engines::velox {

using ComplexExpression = boss::expressions::ComplexExpression;
template <typename... T>
using ComplexExpressionWithStaticArguments =
    boss::expressions::ComplexExpressionWithStaticArguments<T...>;
using Expression = boss::expressions::Expression;
using ExpressionArguments = boss::expressions::ExpressionArguments;
using ExpressionSpanArguments = boss::expressions::ExpressionSpanArguments;
using ExpressionSpanArgument = boss::expressions::ExpressionSpanArgument;
using expressions::generic::ArgumentWrapper;
using expressions::generic::ExpressionArgumentsWithAdditionalCustomAtomsWrapper;

static boss::Expression injectDebugInfoToSpans(boss::Expression&& expr) {
  return std::visit(
      boss::utilities::overload(
          [&](boss::ComplexExpression&& e) -> boss::Expression {
            auto [head, unused_, dynamics, spans] = std::move(e).decompose();
            boss::ExpressionArguments debugDynamics;
            debugDynamics.reserve(dynamics.size() + 1 /*spans.size()*/);
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           std::back_inserter(debugDynamics), [](auto&& arg) {
                             return injectDebugInfoToSpans(std::forward<decltype(arg)>(arg));
                           });
            /*std::transform(
                std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
                std::back_inserter(debugDynamics), [](auto&& span) {
                  return std::visit(
                      [](auto&& typedSpan) -> boss::Expression {
                        using Element = typename std::decay_t<decltype(typedSpan)>::element_type;
                        return boss::ComplexExpressionWithStaticArguments<std::string, int64_t>(
                            "Span"_, {typeid(Element).name(), typedSpan.size()}, {}, {});
                      },
                      std::forward<decltype(span)>(span));
                });*/
            if(!spans.empty()) {
              int64_t numSpans = spans.size();
              int64_t rowCount = std::accumulate(
                  std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
                  (int64_t)0, [](auto sum, auto&& span) {
                    return sum + std::visit([](auto&& typedSpan) { return typedSpan.size(); },
                                            std::forward<decltype(span)>(span));
                  });
              debugDynamics.emplace_back("Span"_(numSpans, rowCount));
            }
            return boss::ComplexExpression(std::move(head), {}, std::move(debugDynamics), {});
          },
          [](auto&& otherTypes) -> boss::Expression { return otherTypes; }),
      std::move(expr));
}

static void outputToFile(std::string const& prefix, std::string const& suffix,
                         boss::Expression&& expr) {
  std::visit(boss::utilities::overload(
                 [&prefix, &suffix](boss::ComplexExpression&& e) {
                   auto [head, unused_, dynamics, spans] = std::move(e).decompose();
                   auto newPrefix = prefix + "_" + head.getName();
                   for(auto&& arg : dynamics) {
                     outputToFile(newPrefix, suffix, std::move(arg));
                   }
                   if(spans.empty()) {
                     return;
                   }
                   std::ofstream spanDataFile;
                   spanDataFile.open(newPrefix + suffix);
                   for(auto&& span : spans) {
                     std::visit(
                         [&spanDataFile](auto&& typedSpan) {
                           for(auto&& v : typedSpan) {
                             spanDataFile << v << std::endl;
                           }
                         },
                         std::move(span));
                   }
                   spanDataFile.close();
                 },
                 [](auto&& otherTypes) {}),
             std::move(expr));
}

struct VeloxVectorPayload {
  VectorPtr vec;
#ifdef TAKE_OWNERSHIP_OF_TASK_POOLS
  std::vector<std::shared_ptr<memory::MemoryPool>> taskPools;
#endif // TAKE_OWNERSHIP_OF_TASK_POOLS

  ~VeloxVectorPayload() {
    // make sure they are released in this order! (pools at the end)
    vec.reset();
#ifdef TAKE_OWNERSHIP_OF_TASK_POOLS
    taskPools.clear();
#endif // TAKE_OWNERSHIP_OF_TASK_POOLS
  }
};

template <typename T> boss::Span<T> createBossSpan(VeloxVectorPayload&& payload) {
  auto* data = const_cast<T*>(payload.vec->values()->as<T>());
  auto length = payload.vec->size();
  return boss::Span<T>(data, length, [p = std::move(payload)]() {});
}

static ExpressionSpanArgument veloxtoSpan(VeloxVectorPayload&& payload) {
  switch(payload.vec->typeKind()) {
  case TypeKind::INTEGER:
    return createBossSpan<int32_t>(std::move(payload));
  case TypeKind::BIGINT:
    return createBossSpan<int64_t>(std::move(payload));
  case TypeKind::DOUBLE:
    return createBossSpan<double_t>(std::move(payload));
  default:
    throw std::runtime_error("veloxToSpan: array type not supported: " +
                             facebook::velox::mapTypeKindToName(payload.vec->typeKind()));
  }
}

template <typename T>
VectorPtr spanToVelox(boss::Span<T>&& span, memory::MemoryPool* pool, BufferPtr& indices) {
  auto createDictVector = [&indices](auto&& flatVecPtr) {
    auto indicesSize = indices->size() / sizeof(int32_t);
    return BaseVector::wrapInDictionary(BufferPtr(nullptr), indices, indicesSize,
                                        std::move(flatVecPtr));
  };

  auto createVeloxVector = [&indices, &createDictVector, &pool](auto&& span, auto bossType) {
    auto spanLength = span.size();
    auto spanBegin = span.begin();
    BossArray bossArray(spanLength, spanBegin, std::move(span));
    auto flatVecPtr = importFromBossAsOwner(bossType, std::move(bossArray), pool);
    if(indices == nullptr) {
      return flatVecPtr;
    }
    return createDictVector(std::move(flatVecPtr));
  };

  if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int32_t const>) {
    return createVeloxVector(std::move(span), BossType::bINTEGER);
  } else if constexpr(std::is_same_v<T, int64_t> || std::is_same_v<T, int64_t const>) {
    return createVeloxVector(std::move(span), BossType::bBIGINT);
  } else if constexpr(std::is_same_v<T, double_t> || std::is_same_v<T, double_t const>) {
    return createVeloxVector(std::move(span), BossType::bDOUBLE);
  }
}

static int32_t dateToInt32(std::string const& str) {
  std::istringstream iss;
  iss.str(str);
  struct std::tm tm = {};
  iss >> std::get_time(&tm, "%Y-%m-%d");
  auto t = std::mktime(&tm);
  return (int32_t)std::chrono::duration_cast<std::chrono::days>(
             std::chrono::system_clock::from_time_t(t).time_since_epoch())
      .count();
}

static Expression dateProcess(Expression&& e) {
  return visit(boss::utilities::overload(
                   [](ComplexExpression&& e) -> Expression {
                     auto head = e.getHead();
                     if(head.getName() == "DateObject") {
                       auto argument = e.getArguments().at(0);
                       std::stringstream out;
                       out << argument;
                       auto dateString = out.str().substr(1, 10);
                       return dateToInt32(dateString);
                     }
                     // at least evaluate all the arguments
                     auto [_, statics, dynamics, spans] = std::move(e).decompose();
                     std::transform(std::make_move_iterator(dynamics.begin()),
                                    std::make_move_iterator(dynamics.end()), dynamics.begin(),
                                    [](auto&& arg) { return dateProcess(std::move(arg)); });
                     return ComplexExpression{std::move(head), std::move(statics),
                                              std::move(dynamics), std::move(spans)};
                   },
                   [](auto&& e) -> Expression { return std::forward<decltype(e)>(e); }),
               std::move(e));
}

template <typename... StaticArgumentTypes>
ComplexExpressionWithStaticArguments<StaticArgumentTypes...>
transformDynamicsToSpans(ComplexExpressionWithStaticArguments<StaticArgumentTypes...>&& input_) {
  std::vector<boss::SpanInputs> spanInputs;
  auto [head, statics, dynamics, oldSpans] = std::move(input_).decompose();

  auto it = std::move_iterator(dynamics.begin());
  for(; it != std::move_iterator(dynamics.end()); ++it) {
    if(!std::visit(
           [&spanInputs]<typename InputType>(InputType&& argument) {
             using Type = std::decay_t<InputType>;
             if constexpr(boss::utilities::isVariantMember<std::vector<Type>,
                                                           boss::SpanInputs>::value) {
               if(!spanInputs.empty() &&
                  std::holds_alternative<std::vector<Type>>(spanInputs.back())) {
                 std::get<std::vector<Type>>(spanInputs.back()).push_back(argument);
               } else {
                 spanInputs.push_back(std::vector<Type>{argument});
               }
               return true;
             }
             return false;
           },
           dateProcess(*it))) {
      break;
    }
  }
  dynamics.erase(dynamics.begin(), it.base());

  if(spanInputs.empty()) {
    return {std::move(head), std::move(statics), {}, std::move(oldSpans)};
  }

  ExpressionSpanArguments spans;
  std::transform(
      std::move_iterator(spanInputs.begin()), std::move_iterator(spanInputs.end()),
      std::back_inserter(spans), [](auto&& untypedInput) {
        return std::visit(
            []<typename Element>(std::vector<Element>&& input) -> ExpressionSpanArgument {
              auto* ptr = input.data();
              auto size = input.size();
              spanReferenceCounter.add(ptr, [v = std::move(input)]() {});
              return boss::Span<Element>(ptr, size, [ptr]() { spanReferenceCounter.remove(ptr); });
            },
            std::forward<decltype(untypedInput)>(untypedInput));
      });

  std::copy(std::move_iterator(oldSpans.begin()), std::move_iterator(oldSpans.end()),
            std::back_inserter(spans));
  return {std::move(head), std::move(statics), std::move(dynamics), std::move(spans)};
}

static Expression transformDynamicsToSpans(Expression&& input) {
  return std::visit(
      [](auto&& x) -> Expression {
        if constexpr(std::is_same_v<std::decay_t<decltype(x)>, ComplexExpression>) {
          return transformDynamicsToSpans(std::forward<decltype(x)>(x));
        } else {
          return x;
        }
      },
      std::move(input));
}

static std::vector<BufferPtr> getIndices(ExpressionSpanArguments&& listSpans) {
  if(listSpans.empty()) {
    throw std::runtime_error("get index error");
  }
  std::vector<BufferPtr> indicesVec;
  std::transform(std::make_move_iterator(listSpans.begin()),
                 std::make_move_iterator(listSpans.end()), std::back_inserter(indicesVec),
                 [](auto&& subSpan) {
                   return std::visit(
                       []<typename T>(boss::Span<T>&& typedSpan) -> BufferPtr {
                         if constexpr(std::is_same_v<T, int32_t>) {
                           auto spanLength = typedSpan.size();
                           auto spanBegin = typedSpan.begin();
                           BossArray bossIndices(spanLength, spanBegin, std::move(typedSpan));
                           return importFromBossAsOwnerBuffer(std::move(bossIndices));
                         } else {
                           throw std::runtime_error("index type error");
                         }
                       },
                       std::move(subSpan));
                 });
  return indicesVec;
}

static std::tuple<std::vector<RowVectorPtr>, RowTypePtr, std::vector<size_t>>
getColumns(ComplexExpression&& expression, memory::MemoryPool* pool) {
  std::vector<RowVectorPtr> rowDataVec;
  std::vector<size_t> spanRowCountVec;
  std::vector<BufferPtr> indicesVec;
  auto indicesColumnEndIndex = size_t(-1);
  std::vector<size_t> otherColumnsSpanIndexes;

  if(expression.getHead().getName() == "Gather") {
    auto [head, statics, dynamics, spans] = std::move(expression).decompose();
    auto it_start = std::move_iterator(dynamics.begin());
    indicesVec = getIndices(std::move(spans));
    expression = std::get<ComplexExpression>(std::move(*it_start));
  }

  if(expression.getHead().getName() == "RadixPartition") {
    auto [head, statics, dynamics, spans] = std::move(expression).decompose();
    auto it = std::move_iterator(dynamics.begin());
    auto it_end = std::move_iterator(dynamics.end());
    auto table = std::get<ComplexExpression>(std::move(*it++));
    auto [tableHead, tableStatic, tableDyn, tableSpans] = std::move(table).decompose();
    indicesColumnEndIndex = tableDyn.size(); // indices don't apply to key columns
    ExpressionSpanArguments indicesSpans;
    ExpressionArguments keyColumns;
    for(; it != it_end; ++it) {
      auto argExpr = get<ComplexExpression>(std::move(*it));
      if(argExpr.getHead().getName() == "Indexes") {
        // not a key column, these are the indices
        assert(indicesSpans.empty());
        auto [unused0, unused1, unused2, argExprSpans] = std::move(argExpr).decompose();
        indicesSpans = std::move(argExprSpans);
        continue;
      }
      // add a key column
      keyColumns.emplace_back(std::move(argExpr));
    }
    if(indicesSpans.empty()) {
      throw std::runtime_error("RadixPartition without Indexes");
    }
    if(indicesSpans.size() == 1 && !tableDyn.empty()) {
      auto const& firstColumn = std::get<ComplexExpression>(tableDyn[0]);
#ifdef USE_NEW_TABLE_FORMAT
      auto const& firstColumnList = get<ComplexExpression>(firstColumn.getDynamicArguments().at(0));
#else
      auto const& firstColumnList = get<ComplexExpression>(firstColumn.getDynamicArguments().at(1));
#endif
      auto const& firstColumnSpans = firstColumnList.getSpanArguments();
      if(firstColumnSpans.size() > 1) {
        // extract the single indices span, convert to shared ptr
        // (so multiple span instances can reference it)
        auto singleIndicesSpan = std::visit(
            []<typename T>(boss::Span<T>&& typedSpan) {
              // std::cout << "make shared indices span " << &typedSpan << std::endl;
              return std::make_shared<ExpressionSpanArgument>(std::move(typedSpan));
            },
            std::move(indicesSpans[0]));
        indicesSpans.clear();
        // extract the key columns, convert them to shared ptr
        // (so multiple span instances can reference it)
        std::vector<std::shared_ptr<ExpressionSpanArgument>> singleKeySpans;
        for(auto& keyColumn : keyColumns) {
          auto [head, unused_, dynamics, spans] =
              std::get<ComplexExpression>(std::move(keyColumn)).decompose();
#ifdef USE_NEW_TABLE_FORMAT
          auto list = get<ComplexExpression>(std::move(dynamics[0]));
#else
          auto list = get<ComplexExpression>(std::move(dynamics[1]));
#endif
          auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
          singleKeySpans.emplace_back(std::visit(
              []<typename T>(boss::Span<T>&& typedSpan) {
                // std::cout << "make shared keys span " << &typedSpan << std::endl;
                return std::make_shared<ExpressionSpanArgument>(std::move(typedSpan));
              },
              std::move(listSpans[0])));
          list = boss::ComplexExpression(std::move(listHead), {}, std::move(listDynamics), {});
#ifdef USE_NEW_TABLE_FORMAT
          dynamics[0] = std::move(list);
#else
          dynamics[1] = std::move(list);
#endif
          keyColumn =
              boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
        }
        // reconstruct the indices and the keys
        std::vector<ExpressionSpanArguments> newKeySpans(singleKeySpans.size());
        std::visit(
            [&]<typename T>(boss::Span<T>& typedIndicesSpan) {
              if constexpr(std::is_same_v<T, int32_t>) {
                std::vector<size_t> spanRanges;
                spanRanges.reserve(firstColumnSpans.size() + 1);
                spanRanges.emplace_back(0);
                std::transform_inclusive_scan(
                    firstColumnSpans.begin(), firstColumnSpans.end(),
                    std::back_inserter(spanRanges), std::plus<size_t>{}, [](auto const& span) {
                      return std::visit([](auto const& typedSpan) { return typedSpan.size(); },
                                        span);
                    });
                auto typedIndicesSpanIt = typedIndicesSpan.begin();
                auto typedIndicesSpanItEnd = typedIndicesSpan.end();
                auto lastIt = typedIndicesSpanIt;
                auto currentSpanIndex = 0;
                auto reconstruct = [&]() {
                  auto length = std::distance(lastIt, typedIndicesSpanIt);
                  if(length == 0) {
                    return;
                  }
                  //  reconstruct the indices
                  //  std::cout << "reconstruct indices span " << singleIndicesSpan.get() <<
                  //  std::endl;
                  indicesSpans.emplace_back(
                      boss::Span<int32_t>(lastIt, length, [v = singleIndicesSpan]() {
                        // std::cout << "indices span destructor: " << v.get() << std::endl;
                      }));
                  // reconstruct the keys
                  auto offset = std::distance(typedIndicesSpan.begin(), lastIt);
                  auto oldKeyIt = singleKeySpans.begin();
                  auto newKeySpansIt = newKeySpans.begin();
                  for(; oldKeyIt != singleKeySpans.end(); ++oldKeyIt, ++newKeySpansIt) {
                    auto& oldSingleKeySpan = *oldKeyIt;
                    std::visit(
                        [&]<typename U>(boss::Span<U> const& oldSingleKeyTypedSpan) {
                          // std::cout << "reconstruct keys span " << oldSingleKeySpan.get()
                          //           << std::endl;
                          newKeySpansIt->emplace_back(
                              boss::Span<U>(oldSingleKeyTypedSpan.begin() + offset, length,
                                            [v = oldSingleKeySpan]() {
                                              // std::cout << "key span destructor: " << v.get()
                                              // << std::endl;
                                            }));
                        },
                        *oldSingleKeySpan);
                  }
                  lastIt = typedIndicesSpanIt;
                  // keep track of span indexes to reconstruct the other columns accordingly
                  otherColumnsSpanIndexes.emplace_back(currentSpanIndex);
                };
                for(; typedIndicesSpanIt != typedIndicesSpanItEnd; ++typedIndicesSpanIt) {
                  auto& index = *typedIndicesSpanIt;
                  if(index >= spanRanges[currentSpanIndex + 1]) {
                    reconstruct();
                    while(index >= spanRanges[currentSpanIndex + 1]) {
                      currentSpanIndex++;
                    }
                  } else if(index < spanRanges[currentSpanIndex]) {
                    reconstruct();
                    while(index < spanRanges[currentSpanIndex]) {
                      currentSpanIndex--;
                    }
                  }
                  // conversion from global to local span index
                  index -= spanRanges[currentSpanIndex];
                }
                if(lastIt != typedIndicesSpanItEnd) {
                  reconstruct();
                }
              } else {
                throw std::runtime_error("index type error");
              }
            },
            *singleIndicesSpan);
        // add the reconstructed key spans into the table
        auto keyColumnsIt = keyColumns.begin();
        for(auto&& newSpans : newKeySpans) {
          auto [head, unused_, dynamics, spans] =
              std::get<ComplexExpression>(std::move(*keyColumnsIt++)).decompose();

#ifdef USE_NEW_TABLE_FORMAT
          auto list = get<ComplexExpression>(std::move(dynamics[0]));
#else
          auto list = get<ComplexExpression>(std::move(dynamics[1]));
#endif
          auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();

          listSpans.clear();
          listSpans.insert(listSpans.end(), std::make_move_iterator(newSpans.begin()),
                           std::make_move_iterator(newSpans.end()));

          list = boss::ComplexExpression(std::move(listHead), {}, std::move(listDynamics),
                                         std::move(listSpans));

#ifdef USE_NEW_TABLE_FORMAT
          dynamics[0] = std::move(list);
#else
          dynamics[1] = std::move(list);
#endif
          tableDyn.emplace_back(
              boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
        }
        keyColumns.clear();
      }
    }
    for(auto&& keyExpr : keyColumns) {
      tableDyn.emplace_back(std::move(keyExpr));
    }
    indicesVec = getIndices(std::move(indicesSpans));

    // get table data
    expression = boss::ComplexExpression(std::move(tableHead), std::move(tableStatic),
                                         std::move(tableDyn), std::move(tableSpans));
  }

  ExpressionArguments columns = std::move(expression).getArguments();
  std::vector<std::string> colNameVec;
  std::vector<std::shared_ptr<Type const>> colTypeVec;
  std::vector<std::vector<VectorPtr>> colDataListVec;

  std::for_each(
      std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
      [&](auto&& columnExpr) {
        auto column = get<ComplexExpression>(std::forward<decltype(columnExpr)>(columnExpr));
        auto [head, unused_, dynamics, spans] = std::move(column).decompose();

#ifdef USE_NEW_TABLE_FORMAT
        auto columnName = head.getName();
        auto dynamic = get<ComplexExpression>(std::move(dynamics.at(0)));
#else
        auto columnName = get<Symbol>(std::move(dynamics.at(0))).getName();
        auto dynamic = get<ComplexExpression>(std::move(dynamics.at(1)));
#endif
        std::transform(columnName.begin(), columnName.end(), columnName.begin(), ::tolower);

        auto list = transformDynamicsToSpans(std::move(dynamic));
        auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
        if(listSpans.empty()) {
          return;
        }

        auto isRadixKeyColumn = bool(colNameVec.size() >= indicesColumnEndIndex);

        auto getVeloxColumnData = [&](auto listSpan, BufferPtr indices) {
          return std::visit(
              [pool, &indices, &columnName]<typename T>(boss::Span<T>&& typedSpan) -> VectorPtr {
                if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                             std::is_same_v<T, double_t> || std::is_same_v<T, int32_t const> ||
                             std::is_same_v<T, int64_t const> || std::is_same_v<T, double_t const>) {
                  return spanToVelox<T>(std::move(typedSpan), pool, indices);
                } else {
                  throw std::runtime_error(
                      "unsupported column type: '" + columnName +
                      "' with type: " + std::string(typeid(decltype(typedSpan)).name()));
                }
              },
              std::move(listSpan));
        };

        /*std::cout << "indicesVec: " << indicesVec.size() << std::endl;
        std::cout << "numSpans: " << listSpans.size() << std::endl;
        std::cout << "otherColumnsSpanIndexes: " << otherColumnsSpanIndexes.size() << std::endl;
        if(!otherColumnsSpanIndexes.empty()) {
          std::cout << "otherColumnsSpanIndexes[0]: " << otherColumnsSpanIndexes[0] << std::endl;
          std::cout << "otherColumnsSpanIndexes.back(): " << otherColumnsSpanIndexes.back()
                    << std::endl;
        }*/

        std::vector<VectorPtr> colDataVec;
        if(isRadixKeyColumn || otherColumnsSpanIndexes.empty()) {
          size_t numIndiceSpan = 0;
          for(auto&& listSpan : listSpans) {
            BufferPtr indices = nullptr;
            if(!indicesVec.empty() && !isRadixKeyColumn) {
              indices = indicesVec[numIndiceSpan++];
            }
            colDataVec.emplace_back(getVeloxColumnData(std::move(listSpan), std::move(indices)));
          }
        } else {
          // re-construct column spans according the the indexes/keys spans
          std::vector<std::shared_ptr<ExpressionSpanArgument>> sharedSpans(
              listSpans.size()); // to be able to re-use spans
          size_t numIndiceSpan = 0;
          for(auto spanIndex : otherColumnsSpanIndexes) {
            BufferPtr indices = nullptr;
            if(!indicesVec.empty() && !isRadixKeyColumn) {
              indices = indicesVec[numIndiceSpan++];
            }
            auto& sharedSpan = sharedSpans[spanIndex];
            if(!sharedSpan) {
              sharedSpan = std::visit(
                  []<typename T>(boss::Span<T>&& typedSpan) {
                    // std::cout << "make shared column span " << &typedSpan << std::endl;
                    return std::make_shared<ExpressionSpanArgument>(std::move(typedSpan));
                  },
                  std::move(listSpans[spanIndex]));
            }
            auto spanRef = std::visit(
                [&sharedSpan]<typename U>(
                    boss::Span<U> const& typedSpanRef) -> ExpressionSpanArgument {
                  // std::cout << "reconstruct column span " << sharedSpan.get() << std::endl;
                  return boss::Span<U>(typedSpanRef.begin(), typedSpanRef.size(),
                                       [v = sharedSpan]() {
                                         // std::cout << "column span destructor: " << v.get() <<
                                         // std::endl;
                                       });
                },
                *sharedSpan);
            colDataVec.emplace_back(getVeloxColumnData(std::move(spanRef), std::move(indices)));
          }
        }

        colNameVec.emplace_back(std::move(columnName));
        colTypeVec.emplace_back(colDataVec[0]->type());
        colDataListVec.push_back(std::move(colDataVec));
      });

  auto tableSchema =
      TypeFactory<TypeKind::ROW>::create(std::move(colNameVec), std::move(colTypeVec));

  if(!colDataListVec.empty()) {
    auto listSize = colDataListVec[0].size();
    for(auto i = 0; i < listSize; i++) {
      spanRowCountVec.push_back(colDataListVec[0][i]->size());
      std::vector<VectorPtr> rowData;
      for(auto j = 0; j < colDataListVec.size(); j++) {
        assert(colDataListVec[j].size() == listSize);
        rowData.push_back(std::move(colDataListVec[j][i]));
      }
      auto rowVector = makeRowVectorNoCopy(tableSchema, std::move(rowData), pool);
      rowDataVec.emplace_back(std::move(rowVector));
    }
  }

  return {std::move(rowDataVec), std::move(tableSchema), std::move(spanRowCountVec)};
}

static std::string projectionExpressionToString(Expression&& e);
static std::string projectionExpressionToString(ComplexExpression&& e) {
  auto [head, unused_, dynamics, unused2_] = std::move(e).decompose();
  if(head.getName() == "Where") {
    auto it = std::make_move_iterator(dynamics.begin());
    return projectionExpressionToString(std::move(*it));
  }
  if(head.getName() == "Equal") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("{} = {}", lhs, rhs);
  }
  if(head.getName() == "StringContainsQ") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("{} like '%{}%'", lhs, rhs);
  }
  if(head.getName() == "DateObject") {
    auto it = std::make_move_iterator(dynamics.begin());
    return std::to_string(dateToInt32(projectionExpressionToString(std::move(*it++))));
  }
  if(head.getName() == "Year") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("cast(((cast({} AS DOUBLE) + 719563.285) / 365.265) AS INTEGER)",
                       projectionExpressionToString(std::move(*it++)));
  }
  if(head.getName() == "Greater") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("({} > {})", lhs, rhs);
  }
  if(head.getName() == "And") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    return std::accumulate(std::next(it), itEnd, projectionExpressionToString(std::move(*it++)),
                           [](std::string&& cumul, auto&& arg) {
                             return fmt::format("({} AND {})", cumul,
                                                projectionExpressionToString(std::move(arg)));
                           });
  }
  if(head.getName() == "Plus") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    return std::accumulate(std::next(it), itEnd, projectionExpressionToString(std::move(*it++)),
                           [](std::string&& cumul, auto&& arg) {
                             return fmt::format("({} + {})", cumul,
                                                projectionExpressionToString(std::move(arg)));
                           });
  }
  if(head.getName() == "Times" || head.getName() == "Multiply") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    return std::accumulate(std::next(it), itEnd, projectionExpressionToString(std::move(*it++)),
                           [](std::string&& cumul, auto&& arg) {
                             return fmt::format("({} * {})", cumul,
                                                projectionExpressionToString(std::move(arg)));
                           });
  }
  if(head.getName() == "Minus" || head.getName() == "Subtract") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("({} - {})", lhs, rhs);
  }
  if(head.getName() == "Divide") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("({} / {})", lhs, rhs);
  }
  if(head.getName() == "Count") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("count({})", projectionExpressionToString(std::move(*it)));
  }
  if(head.getName() == "Sum") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("sum({})", projectionExpressionToString(std::move(*it)));
  }
  if(head.getName() == "Avg") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("avg({})", projectionExpressionToString(std::move(*it)));
  }
  if(head.getName() == "Min") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("min({})", projectionExpressionToString(std::move(*it)));
  }
  if(head.getName() == "Max") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("max({})", projectionExpressionToString(std::move(*it)));
  }
  throw std::runtime_error("Unknown operator: " + head.getName());
}
std::string projectionExpressionToString(Expression&& e) {
  return std::visit(boss::utilities::overload(
                        [](ComplexExpression&& expr) -> std::string {
                          return projectionExpressionToString(std::move(expr));
                        },
                        [](Symbol&& s) -> std::string {
                          auto name = std::move(s).getName();
                          std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                          return {std::move(name)};
                        },
                        [](std::string&& str) -> std::string { return str; },
                        [](auto&& v) -> std::string { return std::to_string(v); }),
                    std::move(e));
}

static std::vector<std::string> expressionToOneSideKeys(Expression&& e) {
  return std::visit(boss::utilities::overload(
                        [](ComplexExpression&& expr) -> std::vector<std::string> {
                          if(expr.getHead().getName() == "List" ||
                             expr.getHead().getName() == "By") {
                            auto [head, unused_, dynamics, unused2_] = std::move(expr).decompose();
                            std::vector<std::string> keys;
                            for(auto&& arg : dynamics) {
                              auto name = std::move(get<Symbol>(arg)).getName();
                              std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                              if(name == "desc") {
                                keys.back() += " desc";
                              } else {
                                keys.emplace_back(std::move(name));
                              }
                            }
                            return std::move(keys);
                          }
                          return {projectionExpressionToString(std::move(expr))};
                        },
                        [](Symbol&& s) -> std::vector<std::string> {
                          auto name = std::move(s).getName();
                          std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                          return {std::move(name)};
                        },
                        [](auto&& v) -> std::vector<std::string> {
                          throw std::runtime_error("unexpected type for join/group key");
                        }),
                    std::move(e));
}

static std::tuple<std::vector<std::string>, std::vector<std::string>>
expressionToJoinKeys(ComplexExpression&& e) {
  auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
  auto it = std::make_move_iterator(dynamics.begin());
  auto buildKeys = expressionToOneSideKeys(std::move(*it++));
  auto probeKeys = expressionToOneSideKeys(std::move(*it++));
  return {std::move(buildKeys), std::move(probeKeys)};
}

static std::vector<std::string> expressionToProjections(ComplexExpression&& e) {
  if(e.getHead().getName() != "As") {
    return {projectionExpressionToString(std::move(e))};
  }
  auto [asHead, unused4_, asDynamics, unused5_] = std::move(e).decompose();
  std::vector<std::string> projections;
  for(auto asIt = std::make_move_iterator(asDynamics.begin());
      asIt != std::make_move_iterator(asDynamics.end()); ++asIt) {
    auto attrName = get<Symbol>(std::move(*asIt++)).getName();
    std::transform(attrName.begin(), attrName.end(), attrName.begin(), ::tolower);
    auto projStr = projectionExpressionToString(std::move(*asIt));
    projections.emplace_back(fmt::format("{} AS {}", projStr, attrName));
  }
  return projections;
}

PlanBuilder Engine::buildOperatorPipeline(
    ComplexExpression&& e, std::vector<std::pair<core::PlanNodeId, size_t>>& scanIds,
    memory::MemoryPool& pool, std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
    int& tableCnt, int& joinCnt) {
  if(e.getHead().getName() == "Table" || e.getHead().getName() == "Gather" ||
     e.getHead().getName() == "RadixPartition") {
    auto tableName = fmt::format("Table{}", tableCnt++);
    auto [rowDataVec, tableSchema, spanRowCountVec] = getColumns(std::move(std::move(e)), &pool);

    auto assignColumns = [](std::vector<std::string> names) {
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignmentsMap;
      assignmentsMap.reserve(names.size());
      for(auto& name : names) {
        assignmentsMap.emplace(name, std::make_shared<BossColumnHandle>(name));
      }
      return assignmentsMap;
    };
    auto assignmentsMap = assignColumns(tableSchema->names());

    core::PlanNodeId scanId;
    auto numSpans = spanRowCountVec.size();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .startTableScan()
                    .outputType(tableSchema)
                    .tableHandle(std::make_shared<BossTableHandle>(
                        kBossConnectorId, std::move(tableName), std::move(tableSchema),
                        std::move(rowDataVec), std::move(spanRowCountVec)))
                    .assignments(assignmentsMap)
                    .endTableScan()
                    .capturePlanNodeId(scanId);
    scanIds.emplace_back(scanId, numSpans);
    return std::move(plan);
  }
  if(e.getHead().getName() == "Project") {
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt, joinCnt);
    auto asExpr = get<ComplexExpression>(std::move(*it++));
    return inputPlan.project(expressionToProjections(std::move(asExpr)));
  }
  if(e.getHead() == "Select"_) {
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt, joinCnt);
    auto predStr = projectionExpressionToString(std::move(*it));
    return inputPlan.filter(predStr);
  }
  if(e.getHead() == "Join"_) {
    ++joinCnt;
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto buildSideInputPlan =
        buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                              planNodeIdGenerator, tableCnt, joinCnt);
    auto probeSideInputPlan =
        buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                              planNodeIdGenerator, tableCnt, joinCnt);
    auto whereExpr = std::move(get<ComplexExpression>(*it));
    auto [unused3_, unused4_, whereDyns, unused5_] = std::move(whereExpr).decompose();
    auto [buildSideKeys, probeSideKeys] =
        expressionToJoinKeys(std::move(get<ComplexExpression>(whereDyns[0])));
    auto const& buildSideLayout = buildSideInputPlan.planNode()->outputType()->names();
    auto const& probeSideLayout = probeSideInputPlan.planNode()->outputType()->names();
    auto outputLayout = buildSideLayout;
    outputLayout.insert(outputLayout.end(), probeSideLayout.begin(), probeSideLayout.end());
    return probeSideInputPlan.hashJoin(probeSideKeys, buildSideKeys, buildSideInputPlan.planNode(),
                                       "", outputLayout);
  }
  if(e.getHead() == "Group"_) {
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt, joinCnt);
    auto secondArg = std::move(*it++);
    auto groupKeysStr =
        it == itEnd ? std::vector<std::string>{} : expressionToOneSideKeys(std::move(secondArg));
    auto asExpr = get<ComplexExpression>(it == itEnd ? std::move(secondArg) : std::move(*it));
    auto aggregates = expressionToProjections(std::move(asExpr));
    return inputPlan.singleAggregation(groupKeysStr, aggregates);
  }
  if(e.getHead() == "Order"_ || e.getHead() == "OrderBy"_ || e.getHead() == "Sort"_ ||
     e.getHead() == "SortBy"_) {
    auto [head, unused_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt, joinCnt);
    auto groupKeysStr = expressionToOneSideKeys(std::move(*it));
    return inputPlan.orderBy(groupKeysStr, true).localMerge(groupKeysStr);
  }
  if(e.getHead() == "Top"_ || e.getHead() == "TopN"_) {
    auto [head, unused_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt, joinCnt);
    auto groupKeysStr = expressionToOneSideKeys(std::move(*it++));
    auto limit = std::holds_alternative<int32_t>(*it) ? std::get<int32_t>(std::move(*it))
                                                      : std::get<int64_t>(std::move(*it));
    return inputPlan.topN(groupKeysStr, limit, true).localMerge(groupKeysStr);
  }
  if(e.getHead() == "Let"_) {
    auto [head, unused_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto subexpr = get<ComplexExpression>(std::move(*it++));
    auto paramExpr = get<ComplexExpression>(std::move(*it++));
    if(paramExpr.getHead() == "Parallel"_) {
      auto paramValue = std::holds_alternative<int32_t>(paramExpr.getDynamicArguments()[0])
                            ? std::get<int32_t>(paramExpr.getDynamicArguments()[0])
                            : std::get<int64_t>(paramExpr.getDynamicArguments()[0]);
      auto oldValue = maxThreads;
      maxThreads = paramValue;
      auto result = buildOperatorPipeline(std::move(subexpr), scanIds, pool, planNodeIdGenerator,
                                          tableCnt, joinCnt);
      maxThreads = oldValue;
      return std::move(result);
    }
    throw std::runtime_error("Unknown Let parameter: " + paramExpr.getHead().getName());
  }
  throw std::runtime_error("Unknown relational operator: " + e.getHead().getName());
}

void veloxPrintResults(std::vector<RowVectorPtr> const& results) {
  std::cout << "Results:" << std::endl;
  bool printType = true;
  for(auto const& vector : results) {
    // Print RowType only once.
    if(printType) {
      std::cout << vector->type()->asRow().toString() << std::endl;
      printType = false;
    }
    for(vector_size_t i = 0; i < vector->size(); ++i) {
      std::cout << vector->toString(i) << std::endl;
    }
  }
}

boss::Expression Engine::evaluate(boss::Expression&& e) {
  if(std::holds_alternative<ComplexExpression>(e)) {
    return evaluate(std::get<ComplexExpression>(std::move(e)));
  }
  return std::move(e);
}

boss::Expression Engine::evaluate(boss::ComplexExpression&& e) {
  if(e.getHead().getName() == "ErrorWhenEvaluatingExpression") {
    return std::move(e);
  }
  if(e.getHead().getName() == "Table") {
    return std::move(e);
  }

  if(e.getHead().getName() == "Set") {
    auto param = std::get<Symbol>(e.getDynamicArguments()[0]);
    if(param == "maxThreads"_) {
      maxThreads = std::holds_alternative<int32_t>(e.getDynamicArguments()[1])
                       ? std::get<int32_t>(e.getDynamicArguments()[1])
                       : std::get<int64_t>(e.getDynamicArguments()[1]);
      return true;
    }
    if(param == "internalBatchNumRows"_) {
      internalBatchNumRows = std::holds_alternative<int32_t>(e.getDynamicArguments()[1])
                                 ? std::get<int32_t>(e.getDynamicArguments()[1])
                                 : std::get<int64_t>(e.getDynamicArguments()[1]);
      return true;
    }
    if(param == "minimumOutputBatchNumRows"_) {
      minimumOutputBatchNumRows = std::holds_alternative<int32_t>(e.getDynamicArguments()[1])
                                      ? std::get<int32_t>(e.getDynamicArguments()[1])
                                      : std::get<int64_t>(e.getDynamicArguments()[1]);
      return true;
    }
    if(param == "HashAdaptivityEnabled"_) {
      hashAdaptivityEnabled = std::get<bool>(e.getDynamicArguments()[1]);
      return true;
    }
    throw std::runtime_error("Unknown Set parameter: " + param.getName());
  }

  static std::mutex m;
  auto pool = [this]() -> std::shared_ptr<memory::MemoryPool> {
    std::lock_guard const lock(m);
    auto& threadPool = threadPools_[std::this_thread::get_id()];
    if(!threadPool) {
      threadPool = memory::MemoryManager::getInstance()->addLeafPool();
    }
    return threadPool;
  }();

  boss::expressions::ExpressionArguments columns;
  auto evalAndAddOutputSpans = [&, this](auto&& e) {
    auto scanIds = std::vector<std::pair<core::PlanNodeId, size_t>>{};
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    int tableCnt = 0;
    int joinCnt = 0;
    auto plan =
        buildOperatorPipeline(std::move(e), scanIds, *pool, planNodeIdGenerator, tableCnt, joinCnt);

    auto params = std::make_unique<CursorParameters>();
    params->planNode = plan.planNode();
    params->maxDrivers = std::max(1, (maxThreads / (joinCnt + 1)) - 1);
    params->copyResult = false;
    std::shared_ptr<folly::Executor> executor;
    if(maxThreads < 2) {
      params->singleThreaded = true;
    } else {
      executor =
          std::make_shared<folly::CPUThreadPoolExecutor>(std::thread::hardware_concurrency());
    }
    auto config = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kHashAdaptivityEnabled, hashAdaptivityEnabled ? "true" : "false"}};
    if(internalBatchNumRows > 0) {
      config[core::QueryConfig::kPreferredOutputBatchRows] = std::to_string(internalBatchNumRows);
      config[core::QueryConfig::kMaxOutputBatchRows] = std::to_string(internalBatchNumRows);
    }
    params->queryCtx =
        std::make_shared<core::QueryCtx>(executor.get(), core::QueryConfig{std::move(config)});

    std::unique_ptr<TaskCursor> cursor;
    auto results = veloxRunQueryParallel(*params, cursor, scanIds);
    if(!cursor) {
      throw std::runtime_error("Query terminated with error");
    }
#ifdef DebugInfo
    veloxPrintResults(results);
    std::cout << std::endl;
    auto& task = cursor->task();
    auto const& stats = task->taskStats();
    std::cout << printPlanWithStats(*params->planNode, stats, false) << std::endl;
#endif // DebugInfo
    if(!results.empty()) {
#ifdef TAKE_OWNERSHIP_OF_TASK_POOLS
      // keep track of which batches have been copied
      // (since they do not need to take ownership of the pools)
      std::vector<bool> copiedResults(results.size());
#endif // TAKE_OWNERSHIP_OF_TASK_POOLS
      std::vector<RowVectorPtr> resultsToCombine;
      size_t currentCombinedResultSize = 0;
      size_t currentCombinedResultIdx = 0;
      auto combine = [&]() {
#ifdef TAKE_OWNERSHIP_OF_TASK_POOLS
        if(resultsToCombine.size() == 1) {
          // avoid copy if no merge is required
          results[currentCombinedResultIdx++] = std::move(resultsToCombine[0]);
          return;
        }
        copiedResults[currentCombinedResultIdx] = true;
#endif // TAKE_OWNERSHIP_OF_TASK_POOLS
        auto copy = BaseVector::create<RowVector>(resultsToCombine[0]->type(),
                                                  currentCombinedResultSize, pool.get());
        size_t combinedStartIdx = 0;
        for(auto&& result : resultsToCombine) {
          copy->copy(result.get(), combinedStartIdx, 0, result->size());
          combinedStartIdx += result->size();
        }
        results[currentCombinedResultIdx++] = std::move(copy);
      };
      for(auto& result : results) {
        if(result->size() > 0) {
          // make sure that lazy vectors are computed
          result->loadedVector();
          // merge results
          currentCombinedResultSize += result->size();
          resultsToCombine.emplace_back(std::move(result));
          if(currentCombinedResultSize >= minimumOutputBatchNumRows) {
            combine();
            currentCombinedResultSize = 0;
            resultsToCombine.clear();
          }
        }
      }
      if(currentCombinedResultSize > 0) {
        combine();
      }
      results.resize(currentCombinedResultIdx);

      auto const& rowType = dynamic_cast<const RowType*>(results[0]->type().get());
      for(int i = 0; i < results[0]->childrenSize(); ++i) {
        ExpressionSpanArguments spans;
        for(int j = 0; j < results.size(); ++j) {
          auto& result = results[j];
          auto& vec = result->childAt(i);
#ifdef TAKE_OWNERSHIP_OF_TASK_POOLS
          if(!copiedResults[j]) {
            BaseVector::flattenVector(vec); // flatten dictionaries
            if(vec->size() > 0) {
              VeloxVectorPayload payload{std::move(vec), cursor->task()->childPools()};
              spans.emplace_back(veloxtoSpan(std::move(payload)));
            }
            continue;
          }
#endif // TAKE_OWNERSHIP_OF_TASK_POOLS
          if(vec->size() > 0) {
            spans.emplace_back(veloxtoSpan({std::move(vec)}));
          }
        }
        auto const& name = rowType->nameOf(i);
        auto listExpr = ComplexExpression{"List"_, {}, {}, std::move(spans)};
#ifdef USE_NEW_TABLE_FORMAT
        boss::expressions::ExpressionArguments args;
        args.emplace_back(std::move(listExpr));
        columns.emplace_back(ComplexExpression(Symbol{name}, {}, std::move(args), {}));
#else
        columns.emplace_back("Column"_(Symbol{name}, std::move(listExpr)));
#endif // USE_NEW_TABLE_FORMAT
      }
    } else {
      // empty result - at least return the empty columns
      auto const& outputColumnNames = plan.planNode()->outputType()->names();
      for(auto const& name : outputColumnNames) {
#ifdef USE_NEW_TABLE_FORMAT
        boss::expressions::ExpressionArguments args;
        args.emplace_back("List"_());
        columns.emplace_back(ComplexExpression(Symbol(name), {}, std::move(args), {}));
#else
        columns.emplace_back("Column"_(Symbol(name), "List"_()));
#endif // USE_NEW_TABLE_FORMAT
      }
    }
  };

  if(e.getHead().getName() == "Union") {
    auto [_, statics, dynamics, spans] = std::move(e).decompose();
    for(auto&& arg : dynamics) {
      evalAndAddOutputSpans(std::move(get<ComplexExpression>(arg)));
    }
  } else {
    evalAndAddOutputSpans(std::move(e));
  }

  auto output = ComplexExpression("Table"_, std::move(columns));

  return std::move(output);
}

Engine::Engine() {
  static bool firstInitialization = true;
  if(firstInitialization) {
    firstInitialization = false;
    // this init is required only when the engine is first loaded (not every engine reset):
    memory::MemoryManager::initialize({});
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    FLAGS_velox_exception_user_stacktrace_enabled = true;
  }
  auto bossConnector =
      connector::getConnectorFactory(boss::engines::velox::BossConnectorFactory::kBossConnectorName)
          ->newConnector(boss::engines::velox::kBossConnectorId, nullptr);
  connector::registerConnector(bossConnector);
}

Engine::~Engine() { connector::unregisterConnector(boss::engines::velox::kBossConnectorId); }

} // namespace boss::engines::velox

static auto& enginePtr(bool initialise = true) {
  static std::mutex m;
  std::lock_guard const lock(m);
  static auto engine = std::unique_ptr<boss::engines::velox::Engine>();
  if(!engine && initialise) {
    engine.reset(new boss::engines::velox::Engine());
  }
  return engine;
}

extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
  auto* r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
}

extern "C" void reset() { enginePtr(false).reset(nullptr); }
