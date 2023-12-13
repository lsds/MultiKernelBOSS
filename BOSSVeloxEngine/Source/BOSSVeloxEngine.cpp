#include "BOSSVeloxEngine.hpp"

#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/exec/PlanNodeStats.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

#include <Utilities.hpp>
#include <any>
#include <iterator>
#include <list>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

#include <ExpressionUtilities.hpp>
#include <cstring>
#include <iostream>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

using std::endl;
using std::to_string;
using std::function;
using std::get;
using std::is_invocable_v;
using std::move;
using std::unordered_map;
using std::string_literals::operator ""s;
using boss::utilities::operator ""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;
using boss::Expression;
using boss::expressions::generic::isComplexExpression;


std::ostream &operator<<(std::ostream &s, std::vector<std::int64_t> const &input /*unused*/) {
    std::for_each(begin(input), prev(end(input)),
                  [&s = s << "["](auto &&element) { s << element << ", "; });
    return (input.empty() ? s : (s << input.back())) << "]";
}

namespace boss {
    using std::vector;
//    using SpanInputs = std::variant<vector<bool>, vector<std::int64_t>, vector<std::double_t>,
//            vector<std::string>, vector<Symbol>>;
    using SpanInputs = std::variant</*vector<bool>, */ std::vector<std::int32_t>, vector<std::int64_t>, vector<std::double_t>,
            vector<std::string>, vector<Symbol>>;
} // namespace boss

namespace boss::engines::velox {
    using VeloxExpressionSystem = ExtensibleExpressionSystem<>;
    using AtomicExpression = VeloxExpressionSystem::AtomicExpression;
    using ComplexExpression = VeloxExpressionSystem::ComplexExpression;
    template<typename... T>
    using ComplexExpressionWithStaticArguments =
            VeloxExpressionSystem::ComplexExpressionWithStaticArguments<T...>;
    using Expression = VeloxExpressionSystem::Expression;
    using ExpressionArguments = VeloxExpressionSystem::ExpressionArguments;
    using ExpressionSpanArguments = VeloxExpressionSystem::ExpressionSpanArguments;
    using ExpressionSpanArgument = VeloxExpressionSystem::ExpressionSpanArgument;
    using expressions::generic::ArgumentWrapper;
    using expressions::generic::ExpressionArgumentsWithAdditionalCustomAtomsWrapper;

// reference counter class to track references for Span and af::array pointers to memory
// calling a destructor once the reference count reaches 0
    class SpanReferenceCounter {
    public:
        // call destructor only for the initial caller of add(), who is the owner of the data
        void add(void *data, std::function<void(void)> &&destructor = {}) {
            auto &info = map.try_emplace(data, std::move(destructor)).first->second;
            info.counter++;
        }

        void remove(void *data) {
            auto it = map.find(data);
            if (--it->second.counter == 0) {
                if (it->second.destructor) {
                    it->second.destructor();
                }
                map.erase(it);
            }
        }

    private:
        struct Info {
            std::function<void(void)> destructor;
            unsigned int counter = 0;

            explicit Info(std::function<void(void)> &&f) : destructor(std::move(f)) {}
        };

        unordered_map<void *, Info> map;
    };

    static SpanReferenceCounter spanReferenceCounter;

    int32_t dateToInt32(std::string str) {
        std::istringstream iss;
        iss.str(str);
        struct std::tm tm = {};
        iss >> std::get_time(&tm, "%Y-%m-%d");
        auto t = std::mktime(&tm);
        return (int32_t) std::chrono::duration_cast<std::chrono::days>(
                std::chrono::system_clock::from_time_t(t).time_since_epoch())
                .count();
    }

    Expression dateProcess(Expression &&e) {
        return visit(boss::utilities::overload(
                             [](ComplexExpression &&e) -> Expression {
                                 auto head = e.getHead();
                                 if (head.getName() == "DateObject") {
                                     auto argument = e.getArguments().at(0);
                                     std::stringstream out;
                                     out << argument;
                                     std::string dateString = out.str().substr(1, 10);
                                     return dateToInt32(dateString);
                                 }
                                 // at least evaluate all the arguments
                                 auto [_, statics, dynamics, spans] = std::move(e).decompose();
                                 std::transform(std::make_move_iterator(dynamics.begin()),
                                                std::make_move_iterator(dynamics.end()),
                                                dynamics.begin(),
                                                [](auto &&arg) { return dateProcess(std::move(arg)); });
                                 return ComplexExpression{std::move(head), std::move(statics),
                                                          std::move(dynamics), std::move(spans)};
                             },
                             [](auto &&e) -> Expression { return std::forward<decltype(e)>(e); }),
                     std::move(e));
    }

    template<typename... StaticArgumentTypes>
    ComplexExpressionWithStaticArguments<StaticArgumentTypes...>
    transformDynamicsToSpans(ComplexExpressionWithStaticArguments<StaticArgumentTypes...> &&input_) {
        std::vector<boss::SpanInputs> spanInputs;
        auto [head, statics, dynamics, oldSpans] = std::move(input_).decompose();

        auto it = std::move_iterator(dynamics.begin());
        for (; it != std::move_iterator(dynamics.end()); ++it) {
            if (!std::visit(
                    [&spanInputs]<typename InputType>(InputType &&argument) {
                        using Type = std::decay_t<InputType>;
                        if constexpr (boss::utilities::isVariantMember<std::vector<Type>,
                                boss::SpanInputs>::value) {
                            if (!spanInputs.empty() &&
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

        ExpressionSpanArguments spans;
        std::transform(
                std::move_iterator(spanInputs.begin()), std::move_iterator(spanInputs.end()),
                std::back_inserter(spans), [](auto &&untypedInput) {
                    return std::visit(
                            []<typename Element>(std::vector<Element> &&input) -> ExpressionSpanArgument {
//                                return boss::Span<Element>(std::move(input));
                                auto *ptr = input.data();
                                auto size = input.size();
                                spanReferenceCounter.add(ptr, [v = std::move(input)]() {});
                                return boss::Span<Element>(ptr, size,
                                                           [ptr]() { spanReferenceCounter.remove(ptr); });
                            },
                            std::forward<decltype(untypedInput)>(untypedInput));
                });

        std::copy(std::move_iterator(oldSpans.begin()), std::move_iterator(oldSpans.end()),
                  std::back_inserter(spans));
        return {std::move(head), std::move(statics), std::move(dynamics), std::move(spans)};
    }

    Expression transformDynamicsToSpans(Expression &&input) {
        return std::visit(
                [](auto &&x) -> Expression {
                    if constexpr (std::is_same_v<std::decay_t<decltype(x)>, ComplexExpression>) {
                        return transformDynamicsToSpans(std::forward<decltype(x)>(x));
                    } else {
                        return x;
                    }
                },
                std::move(input));
    }

    template<typename VisiteeT, typename... VisitorTs>
    decltype(auto) visitArgument(VisiteeT &&visitee, VisitorTs... visitors) {
        return std::visit(boss::utilities::overload(
                                  std::forward<decltype(visitors)>(visitors)...,
                                  [](auto &&e) -> Expression { return std::forward<decltype(e)>(e); }),
                          std::forward<decltype(visitee)>(visitee));
    }

    template<typename T>
    VectorPtr spanToVelox(boss::Span<T> &&span, memory::MemoryPool *pool, BufferPtr indices = nullptr) {
        BossArray bossArray(span.size(), span.begin(), std::move(span));
        auto createDictVector = [](BufferPtr indices, auto flatVecPtr) {
            auto indicesSize = indices->size() / 4;
            return BaseVector::wrapInDictionary(
                    BufferPtr(nullptr), indices, indicesSize, std::move(flatVecPtr));
        };

        if constexpr (std::is_same_v<T, int32_t>) {
            auto flatVecPtr = importFromBossAsOwner(BossType::bINTEGER, bossArray, pool);
            if (indices == nullptr)
                return flatVecPtr;
            return createDictVector(indices, flatVecPtr);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            auto flatVecPtr = importFromBossAsOwner(BossType::bBIGINT, bossArray, pool);
            if (indices == nullptr)
                return flatVecPtr;
            return createDictVector(indices, flatVecPtr);
        } else if constexpr (std::is_same_v<T, double_t>) {
            auto flatVecPtr = importFromBossAsOwner(BossType::bDOUBLE, bossArray, pool);
            if (indices == nullptr)
                return flatVecPtr;
            return createDictVector(indices, flatVecPtr);
        }
    }

    ExpressionSpanArgument veloxtoSpan(VectorPtr &&vec) {
        if (vec->typeKind() == TypeKind::INTEGER) {
            auto const *data = vec->values()->as<int32_t>();
            auto length = vec->size();
            return boss::Span<const int32_t>(data, length, [v = std::move(vec)]() {});
        } else if (vec->typeKind() == TypeKind::BIGINT) {
            auto const *data = vec->values()->as<int64_t>();
            auto length = vec->size();
            return boss::Span<const int64_t>(data, length, [v = std::move(vec)]() {});
        } else if (vec->typeKind() == TypeKind::DOUBLE) {
            auto const *data = vec->values()->as<double_t>();
            auto length = vec->size();
            return boss::Span<const double_t>(data, length, [v = std::move(vec)]() {});
        } else {
            throw std::runtime_error(
                    "veloxToSpan: array type not supported: " + facebook::velox::mapTypeKindToName(vec->typeKind()));
        }
    }

    bool cmpFunCheck(std::string input) {
        static std::vector<std::string> cmpFun{"Greater", "Equal", "StringContainsQ"};
        for (int i = 0; i < cmpFun.size(); i++) {
            if (input == cmpFun[i]) {
                return 1;
            }
        }
        return 0;
    }

    void formatVeloxProjection(std::vector<std::string> &projectionList,
                               std::unordered_map<std::string, std::string> projNameMap) {
        std::vector<std::string> input(3);
        std::string out;
        for (int i = 0; i < 3; i++) {
            auto tmp = projectionList.back();
            auto it = projNameMap.find(tmp);
            if (it != projNameMap.end())
                tmp = it->second;
            input[2 - i] = tmp;
            projectionList.pop_back();
        }

        if (input[0] == "Multiply" || input[0] == "Times") {
            out = fmt::format("({}) * ({})", input[1], input[2]);
        } else if (input[0] == "Plus") {
            out = fmt::format("({}) + ({})", input[1], input[2]);
        } else if (input[0] == "Minus" || input[0] == "Subtract") {
            out = fmt::format("({}) - ({})", input[1], input[2]);
        } else VELOX_FAIL("unexpected Projection type");
        projectionList.push_back(out);
    }

    void bossExprToVeloxFilter_Join(Expression &&expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](auto a) {
                            AtomicExpr element;
                            element.data = to_string(a);
                            element.type = cValue;
                            queryBuilder.tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](char const *a) {
                            AtomicExpr element;
                            element.data = a;
                            element.type = cValue;
                            queryBuilder.tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](Symbol const &a) {
                            if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                          queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                          a.getName()) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                queryBuilder.curVeloxExpr.selectedColumns.push_back(a.getName());
                            AtomicExpr element;
                            element.data = a.getName();
                            element.type = cName;
                            queryBuilder.tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](std::string const &a) {
                            AtomicExpr element;
                            element.data = "'" + a + "'";
                            element.type = cValue;
                            queryBuilder.tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](ComplexExpression &&expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            if (cmpFunCheck(headName)) { // field filter or join op pre-process
                                queryBuilder.tmpFieldFilter.clear();
                                queryBuilder.tmpFieldFilter.opName = headName;
                            }

                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();
                            auto it = std::move_iterator(dynamics.begin());
                            for (; it != std::move_iterator(dynamics.end()); ++it) {
                                auto argument = *it;
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                std::stringstream out;
                                out << argument;
                                std::string tmpArhStr = out.str();

                                if (headName == "List") {
                                    if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                                  queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                                  tmpArhStr) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                        queryBuilder.curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                    if (!queryBuilder.tmpJoinPairList.leftFlag)
                                        queryBuilder.tmpJoinPairList.leftKeys.emplace_back(tmpArhStr);
                                    else
                                        queryBuilder.tmpJoinPairList.rightKeys.emplace_back(tmpArhStr);
                                    continue;
                                }

                                if (tmpArhStr.substr(0, 10) == "DateObject") {
                                    std::string dateString = tmpArhStr.substr(12, 10);

                                    auto dateInt32 = dateToInt32(dateString);

                                    AtomicExpr element;
                                    element.data = to_string(dateInt32);
                                    element.type = cValue;
                                    queryBuilder.tmpFieldFilter.element.emplace_back(element);
                                    continue;
                                }
                                if (tmpArhStr.substr(0, 4) == "Date") {
                                    std::string dateString = tmpArhStr.substr(6, 10);
                                    auto dateInt32 = dateToInt32(dateString);

                                    AtomicExpr element;
                                    element.data = to_string(dateInt32);
                                    element.type = cValue;
                                    queryBuilder.tmpFieldFilter.element.emplace_back(element);
                                    continue;
                                }

                                bossExprToVeloxFilter_Join(std::move(argument), queryBuilder);
                            }
                            if (!queryBuilder.tmpFieldFilter.opName.empty()) { // field filter or join op post-process
                                if (headName == "Greater")
                                    queryBuilder.mergeGreaterFilter(queryBuilder.tmpFieldFilter);
                                else if ((headName == "Equal" || headName == "StringContainsQ") &&
                                         !queryBuilder.tmpFieldFilter.element.empty()) {
                                    queryBuilder.curVeloxExpr.tmpFieldFiltersVec.push_back(queryBuilder.tmpFieldFilter);
                                }
                            }
                            if (headName == "List") {
                                if (!queryBuilder.tmpJoinPairList.leftFlag)
                                    queryBuilder.tmpJoinPairList.leftFlag = true;
                                else {
                                    queryBuilder.curVeloxExpr.hashJoinListVec.push_back(queryBuilder.tmpJoinPairList);
                                    if (!queryBuilder.curVeloxExpr.hashJoinVec.empty())
                                        queryBuilder.curVeloxExpr.delayJoinList = true;
                                    queryBuilder.tmpJoinPairList.clear();
                                }
                            }
                        }),
                std::move(expression));
    }

    void bossExprToVeloxProj_PartialAggr(Expression &&expression, std::vector<std::string> &projectionList,
                                         QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](auto a) {
                            projectionList.push_back(to_string(a));
                        },
                        [&](char const *a) {
                            projectionList.push_back(a);
                        },
                        [&](Symbol const &a) {
                            if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                          queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                          a.getName()) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                queryBuilder.curVeloxExpr.selectedColumns.push_back(a.getName());
                            projectionList.push_back(a.getName());
                        },
                        [&](std::string const &a) {
                            projectionList.push_back(a);
                        },
                        [&](ComplexExpression &&expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            projectionList.push_back(headName);

                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();
                            auto it = std::move_iterator(dynamics.begin());
                            for (; it != std::move_iterator(dynamics.end()); ++it) {
                                auto argument = *it;
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                if (headName == "Sum" || headName == "Avg" || headName == "Count") {
                                    std::stringstream out;
                                    out << argument;
                                    std::string tmpArhStr = out.str();
                                    aggrPair aggregation;
                                    auto str = headName;
                                    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
                                    aggregation.op = str;
                                    aggregation.oldName = tmpArhStr;
                                    aggregation.newName = "";
                                    queryBuilder.curVeloxExpr.aggregatesVec.push_back(aggregation);
                                    if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                                  queryBuilder.curVeloxExpr.selectedColumns.end(),
                                                  tmpArhStr) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                        queryBuilder.curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                    continue;
                                }

                                bossExprToVeloxProj_PartialAggr(std::move(argument), projectionList, queryBuilder);
                                if (headName == "Year") {
//                              auto out = fmt::format("year({})", projectionList.back());
                                    // Date type has already been treated as int64
                                    auto out = fmt::format(
                                            "cast(((cast({} AS DOUBLE) + 719563.285) / 365.265) AS BIGINT)",
                                            projectionList.back());
                                    projectionList.pop_back();
                                    projectionList.pop_back();
                                    projectionList.push_back(out);
                                }
                            }
                            if (projectionList.size() >= 3) { // field filter or join op post-process
                                formatVeloxProjection(projectionList, queryBuilder.projNameMap);
                            }
                        }),
                std::move(expression));
    }

    void bossExprToVeloxBy(Expression &&expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](Symbol const &a) {
                            if (queryBuilder.curVeloxExpr.orderBy) {
                                if (!strcasecmp(a.getName().c_str(), "desc")) {
                                    auto it = queryBuilder.curVeloxExpr.orderByVec.end() - 1;
                                    *it = *it + " DESC";
                                } else {
                                    auto it = queryBuilder.aggrNameMap.find(a.getName());
                                    if (it != queryBuilder.aggrNameMap.end())
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(it->second);
                                    else
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(a.getName());
                                }
                            } else {
                                if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                              queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                              a.getName()) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                    queryBuilder.curVeloxExpr.selectedColumns.push_back(a.getName());
                                queryBuilder.curVeloxExpr.groupingKeysVec.push_back(a.getName());
                            }
                        },
                        [&](ComplexExpression &&expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();
                            auto it = std::move_iterator(dynamics.begin());
                            for (; it != std::move_iterator(dynamics.end()); ++it) {
                                auto argument = *it;
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                bossExprToVeloxBy(std::move(argument), queryBuilder);
                            }
                        },
                        [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
                std::move(expression));
    }

// "Table"_("Column"_("Id"_, "List"_(1, 2, 3), "Column"_("Value"_ "List"_(0.1, 10.0, 5.2)))
    std::unordered_map<std::string, TypePtr> getColumns(
            ComplexExpression &&expression, std::vector<BufferPtr> indicesVec,
            std::vector<RowVectorPtr> &rowDataVec, memory::MemoryPool *pool) {
        ExpressionArguments columns = std::move(expression).getArguments();
        std::vector<std::string> colNameVec;
        std::vector<std::vector<VectorPtr>> colDataListVec;
        std::unordered_map<std::string, TypePtr> fileColumnNamesMap(columns.size());

        std::for_each(
                std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
                [&colNameVec, &colDataListVec, pool, &fileColumnNamesMap, indicesVec](
                        auto &&columnExpr) {
                    auto column = get < ComplexExpression > (std::forward<decltype(columnExpr)>(columnExpr));
                    auto [head, unused_, dynamics, spans] = std::move(column).decompose();
                    auto columnName = get < Symbol > (std::move(dynamics.at(0)));
                    auto dynamic = get < ComplexExpression > (std::move(dynamics.at(1)));
                    auto list = transformDynamicsToSpans(std::move(dynamic));
                    auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
                    if (listSpans.empty()) {
                        return;
                    }

                    int numSpan = 0;
                    std::vector<VectorPtr> colDataVec;
                    for (auto &subSpan: listSpans) {
                        BufferPtr indices = nullptr;
                        if (indicesVec.size() > 0) {
                            indices = indicesVec[numSpan++];
                        }
                        VectorPtr subColData = std::visit(
                                [pool, indices]<typename T>(boss::Span<T> &&typedSpan) -> VectorPtr {
                                    if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                                                  std::is_same_v<T, double_t>) {
                                        return spanToVelox<T>(std::move(typedSpan), pool, indices);
                                    } else {
                                        throw std::runtime_error("unsupported column type in Select");
                                    }
                                },
                                std::move(subSpan));
                        colDataVec.push_back(std::move(subColData));
                    }
                    auto columnType = colDataVec[0]->type();
                    colNameVec.emplace_back(columnName.getName());
                    fileColumnNamesMap.insert(std::make_pair(columnName.getName(), columnType));
                    colDataListVec.push_back(std::move(colDataVec));
                });

        auto listSize = colDataListVec[0].size();
        for (auto i = 0; i < listSize; i++) {
            std::vector<VectorPtr> rowData;
            for (auto j = 0; j < columns.size(); j++) {
                assert(colDataListVec[j].size() == listSize);
                rowData.push_back(std::move(colDataListVec[j][i]));
            }
            auto rowVector = makeRowVector(colNameVec, rowData, pool);
            rowDataVec.push_back(std::move(rowVector));
        }

        return fileColumnNamesMap;
    }

    std::vector<BufferPtr> getIndices(ExpressionSpanArguments &&listSpans, memory::MemoryPool *pool) {
        if (listSpans.empty()) {
            throw std::runtime_error("get index error");
        }

        std::vector<BufferPtr> indicesVec;
        for (auto &subSpan: listSpans) {
            BufferPtr indexData = std::visit(
                    [pool]<typename T>(boss::Span<T> &&typedSpan) -> BufferPtr {
                        if constexpr (std::is_same_v<T, int32_t>) {
                            BossArray bossIndices(typedSpan.size(), typedSpan.begin(), std::move(typedSpan));
                            return importFromBossAsOwnerBuffer(bossIndices);
                        } else {
                            throw std::runtime_error("index type error");
                        }
                    },
                    std::move(subSpan));
            indicesVec.push_back(std::move(indexData));
        }
        return indicesVec;
    }

    void bossExprToVelox(Expression &&expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](std::int32_t a) {
                            queryBuilder.curVeloxExpr.limit = a;
                        },
                        [&](ComplexExpression &&expression) {
                            std::string projectionName;
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            if (headName == "Table") {
                                if (!queryBuilder.curVeloxExpr.tableName.empty()) // traverse for a new plan builder, triggered by Table head
                                {
                                    queryBuilder.veloxExprList.push_back(queryBuilder.curVeloxExpr);
                                    queryBuilder.curVeloxExpr.clear();
                                }
                                queryBuilder.curVeloxExpr.tableName = fmt::format("Table{}",
                                                                                  queryBuilder.tableCnt++); // indicate table names
                                queryBuilder.curVeloxExpr.fileColumnNamesMap = getColumns(std::move(expression),
                                                                                          queryBuilder.curVeloxExpr.indicesVec,
                                                                                          queryBuilder.curVeloxExpr.rowDataVec,
                                                                                          queryBuilder.pool_.get());
                                return;
                            }

                            std::vector<std::string> lastProjectionsVec;
                            if (headName ==
                                "As") { //save the latest ProjectionsVec to restore for AS - aggregation
                                lastProjectionsVec = queryBuilder.curVeloxExpr.projectionsVec;
                                queryBuilder.curVeloxExpr.projectionsVec.clear();
                            }

                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();

                            auto it_start = std::move_iterator(dynamics.begin());
                            auto it = std::move_iterator(dynamics.begin());

                            if (headName == "Gather") {
                                queryBuilder.curVeloxExpr.indicesVec = getIndices(std::move(oldSpans),
                                                                                  queryBuilder.pool_.get());
                                bossExprToVelox(std::move(*it_start), queryBuilder);
                                return;
                            }

                            for (; it != std::move_iterator(dynamics.end()); ++it) {
                                auto argument = *it;
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                auto toString = [](auto &&arg) {
                                    std::stringstream out;
                                    out << arg;
                                    return out.str();
                                };

                                if (headName == "Where") {
                                    bossExprToVeloxFilter_Join(std::move(argument), queryBuilder);
                                    if (!queryBuilder.curVeloxExpr.tmpFieldFiltersVec.empty())
                                        queryBuilder.formatVeloxFilter_Join();
                                } else if (headName == "As") {
                                    if ((it - it_start) % 2 == 0) {
                                        projectionName = toString(argument);
                                    } else {
                                        std::vector<std::string> projectionList;
                                        bossExprToVeloxProj_PartialAggr(std::move(argument), projectionList,
                                                                        queryBuilder);

                                        // fill the new name for aggregation
                                        if (!queryBuilder.curVeloxExpr.aggregatesVec.empty()) {
                                            auto &aggregation = queryBuilder.curVeloxExpr.aggregatesVec.back();
                                            if (aggregation.newName == "") {
                                                aggregation.newName = projectionName;
                                                queryBuilder.curVeloxExpr.orderBy = true;
                                            }
                                            queryBuilder.curVeloxExpr.projectionsVec = lastProjectionsVec;
                                        } else {
                                            std::string projection;
                                            if (projectionList[0] != projectionName)
                                                projection = fmt::format("{} AS {}", projectionList[0],
                                                                         projectionName);
                                            else {
                                                auto it = queryBuilder.projNameMap.find(projectionName);
                                                if (it != queryBuilder.projNameMap.end() && it->second != it->first) {
                                                    auto tmp = fmt::format("{} AS {}", it->second, it->first);
                                                    projection = tmp; // e.g. cal AS cal
                                                } else
                                                    projection = projectionList[0]; // e.g. col0 AS col0
                                            }
                                            queryBuilder.curVeloxExpr.projectionsVec.push_back(projection);
                                            // avoid repeated projectionMap
                                            auto it = queryBuilder.projNameMap.find(projectionName);
                                            if (it == queryBuilder.projNameMap.end())
                                                queryBuilder.projNameMap.emplace(projectionName, projectionList[0]);
                                        }
                                    }
                                } else if (headName == "By") {
                                    bossExprToVeloxBy(std::move(argument), queryBuilder);
                                } else if (headName == "Sum") {
                                    auto aName = fmt::format("a{}", queryBuilder.aggrNameMap.size());
                                    aggrPair aggregation;
                                    aggregation.op = "sum";
                                    aggregation.oldName = toString(argument);
                                    aggregation.newName = aName;
                                    queryBuilder.curVeloxExpr.aggregatesVec.push_back(aggregation);
                                    // new implicit name for maybe later orderBy
                                    queryBuilder.aggrNameMap.emplace(aggregation.oldName, aName);
                                    queryBuilder.curVeloxExpr.orderBy = true;
                                    if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                                  queryBuilder.curVeloxExpr.selectedColumns.end(),
                                                  aggregation.oldName) ==
                                        queryBuilder.curVeloxExpr.selectedColumns.end())
                                        queryBuilder.curVeloxExpr.selectedColumns.push_back(aggregation.oldName);
                                } else {
                                    bossExprToVelox(std::move(argument), queryBuilder);
                                }
                            }
                        },
                        [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
                std::move(expression));
    }

    boss::Expression Engine::evaluate(boss::Expression &&e) {
        if (std::holds_alternative<ComplexExpression>(e)) {
            return evaluate(std::get<ComplexExpression>(std::move(e)));
        }
        return std::move(e);
    }

    boss::Expression Engine::evaluate(boss::ComplexExpression &&e) {
        QueryBuilder queryBuilder;
        bossExprToVelox(std::move(e), queryBuilder);
        if (queryBuilder.tableCnt) {
            queryBuilder.getFileColumnNamesMap();
            queryBuilder.reformVeloxExpr();
            auto planPtr = queryBuilder.getVeloxPlanBuilder();

#ifdef DebugInfo
            std::cout << planPtr->toString(true, true) << std::endl;
#endif

            params.planNode = planPtr;
            auto results = runQuery(params, cursor);
            if (!cursor) {
                throw std::runtime_error("Query terminated with error");
            }
#ifdef DebugInfo
            printResults(results);
            std::cout << std::endl;
            auto task = cursor->task();
            const auto stats = task->taskStats();
            std::cout << printPlanWithStats(*planPtr, stats, false) << std::endl;
#endif
            ExpressionSpanArguments newSpans;
            if (!results.empty()) {
                for (int i = 0; i < results[0]->childrenSize(); ++i) {
                    for (int j = 0; j < results.size(); ++j) {
                        newSpans.emplace_back(veloxtoSpan(std::move(results[j]->childAt(i))));
                    }
                }
            }

            auto bossResults = boss::expressions::ExpressionArguments();
            bossResults.push_back(ComplexExpression("List"_, {}, {}, std::move(newSpans)));
            return ComplexExpression("List"_, std::move(bossResults));
        } else {
            return std::move(e);
        }
    }

} // namespace boss::engines::velox

static auto &enginePtr(bool initialise = true) {
    static auto engine = std::unique_ptr<boss::engines::velox::Engine>();
    if (!engine && initialise) {
        engine.reset(new boss::engines::velox::Engine());
        engine->executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
                std::thread::hardware_concurrency());
        engine->params.queryCtx = std::make_shared<core::QueryCtx>(engine->executor_.get());
        functions::prestosql::registerAllScalarFunctions();
        aggregate::prestosql::registerAllAggregateFunctions();
        parse::registerTypeResolver();
    }
    return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
    static std::mutex m;
    std::lock_guard lock(m);
    auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
    return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
