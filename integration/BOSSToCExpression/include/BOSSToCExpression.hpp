#pragma once
#include "utils.hpp"
// #include "Translator.hpp"
#include "ScalarTranslatorBase.hpp"
#include "TranslatorBase.hpp"
#include "gpoptextender/Translation/B2CConverter.hpp"

namespace bosstocexpression {
// class Translator;
// class ScalarTranslator;
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class TranslatorBase;

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType, typename InpScalarAuxType>
class ScalarTranslatorBase;

template <typename RetAuxType>
struct RetType;

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType,
          typename InpScalarAuxType>
class BOSSToCExpressionConverter : public orcaextender::B2CConverter {
 public:
  using Translator = TranslatorBase<RetAuxType, InpAuxType, RetScalarAuxType, InpScalarAuxType>;
  using ScalarTranslator = ScalarTranslatorBase<RetAuxType, InpAuxType, RetScalarAuxType, InpScalarAuxType>;

  BOSSToCExpressionConverter() {
    translatorId = 0;
    scalarTranslatorId = 0;
  };
  ~BOSSToCExpressionConverter() = default;

  // Entry point
  RetType<RetAuxType> Convert(Expression&& bossExpr,
                                              InpAuxType const& aux) {
    return std::visit(
        boss::utilities::overload(
            [&](ComplexExpression&& cexpr) -> RetType<RetAuxType> {
              // ComplexExpression expr = std::move(cexpr);
              auto [translators, ogExpr] =
                  GetOrderedRelevantTranslators(std::move(cexpr));
              for (auto& translator : translators) {
                  RetType<RetAuxType> ret = translator->Translate(std::move(ogExpr), *this, aux);
                  if (ret.success) {
                    return ret;
                  }
              }
              return {nullptr, false};
            },
            [&](auto&& val) -> RetType<RetAuxType> {
              std::cerr << "Unsupported expression type" << std::endl;
              throw std::runtime_error("Unsupported expression type");
            }),
        std::move(bossExpr));
  };

  RetType<RetScalarAuxType> ConvertScalar(
      Expression&& scalarExpr, InpScalarAuxType const& aux) {
    auto [translators, ogScalarExpr] =
        GetOrderedRelevantScalarTranslators(std::move(scalarExpr));
    for (auto& translator : translators) {
        RetType<RetScalarAuxType> ret = translator->Translate(std::move(ogScalarExpr), *this, aux);
        if (ret.success) {
          return ret;
        }
    }

    return {nullptr, false};
  };

  ULONG RegisterTranslator(std::unique_ptr<Translator> translator) {
    ULONG id = translatorId++;
    translators[id] = std::move(translator);
    return id;
  };
  ULONG RegisterScalarTranslator(std::unique_ptr<ScalarTranslator> translator) {
    ULONG id = scalarTranslatorId++;
    scalarTranslators[id] = std::move(translator);
    return id;
  };

  void RemoveTranslator(ULONG translatorID) override {
    translators.erase(translatorID);
  };
  void RemoveScalarTranslator(ULONG translatorID) override {
    scalarTranslators.erase(translatorID);
  };

 private:
  ULONG translatorId;
  ULONG scalarTranslatorId;
  std::unordered_map<ULONG, std::unique_ptr<Translator>> translators;
  std::unordered_map<ULONG, std::unique_ptr<ScalarTranslator>> scalarTranslators;

  std::vector<Translator*> GetAllTranslators() {
    std::vector<Translator*> allTranslators;
    for (auto& [_, translator] : translators) {
      allTranslators.push_back(translator.get());
    }
    return allTranslators;
  };

  std::vector<ScalarTranslator*> GetAllScalarTranslators() {
    std::vector<ScalarTranslator*> allScalarTranslators;
    for (auto& [_, translator] : scalarTranslators) {
      allScalarTranslators.push_back(translator.get());
    }
    return allScalarTranslators;
  };

  std::pair<std::vector<Translator*>, ComplexExpression> GetOrderedRelevantTranslators(
      ComplexExpression&& expr) {
    std::vector<std::pair<int, Translator*>> priorityRelevantTranslators;
    for (Translator* translator : GetAllTranslators()) {
      auto matchResult = translator->Match(std::move(expr));
      if (matchResult.first) {
        priorityRelevantTranslators.push_back(std::make_pair(translator->GetPriority(), translator));
      }
      expr = std::move(matchResult.second);
    }
    std::sort(priorityRelevantTranslators.begin(), priorityRelevantTranslators.end(), [](const std::pair<int, Translator*>& a, const std::pair<int, Translator*>& b) {
      return a.first > b.first;
    });

    std::vector<Translator*> orderedRelevantTranslators;
    for (auto& [_, translator] : priorityRelevantTranslators) {
      orderedRelevantTranslators.push_back(translator);
    }
    return std::make_pair(orderedRelevantTranslators, std::move(expr));
  };

  std::pair<std::vector<ScalarTranslator*>, Expression> GetOrderedRelevantScalarTranslators(
      Expression&& expr) {
    std::vector<std::pair<int, ScalarTranslator*>> priorityRelevantScalarTranslators;
    for (ScalarTranslator* translator : GetAllScalarTranslators()) {
      auto matchResult = translator->Match(std::move(expr));
      if (matchResult.first) {
        priorityRelevantScalarTranslators.push_back(std::make_pair(translator->GetPriority(), translator));
      }
      expr = std::move(matchResult.second);
    }
    std::sort(priorityRelevantScalarTranslators.begin(), priorityRelevantScalarTranslators.end(), [](const std::pair<int, ScalarTranslator*>& a, const std::pair<int, ScalarTranslator*>& b) {
      return a.first > b.first;
    });

    std::vector<ScalarTranslator*> orderedRelevantScalarTranslators;
    for (auto& [_, translator] : priorityRelevantScalarTranslators) {
      orderedRelevantScalarTranslators.push_back(translator);
    }
    return std::make_pair(orderedRelevantScalarTranslators, std::move(expr));
  };


};
}  // namespace bosstocexpression
