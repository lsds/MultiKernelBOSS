#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ScalarTranslatorBase.hpp"
#include "TranslatorBase.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "gpoptextender/Translation/C2BConverter.hpp"

namespace cexpressiontoboss {
namespace translation {
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType,
          typename InpScalarAuxType>
class TranslatorBase;

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType,
          typename InpScalarAuxType>
class ScalarTranslatorBase;

template <typename RetAuxType>
struct RetTypeC2B;
}  // namespace translation

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType,
          typename InpScalarAuxType>
class CExpressionToBOSSConverter : public orcaextender::C2BConverter {
 public:
  using Translator =
      translation::TranslatorBase<RetAuxType, InpAuxType, RetScalarAuxType,
                                  InpScalarAuxType>;
  using ScalarTranslator =
      translation::ScalarTranslatorBase<RetAuxType, InpAuxType,
                                        RetScalarAuxType, InpScalarAuxType>;


  CExpressionToBOSSConverter() = default;
  ~CExpressionToBOSSConverter() = default;

  translation::RetTypeC2B<RetAuxType> Convert(CExpression* pexpr,
                                              InpAuxType const& aux) {
    if (!pexpr) {
      return {{}, false};
    }

    std::vector<Translator*> orderedRelevantTranslators =
        GetOrderedRelevantTranslators(pexpr);
    for (const auto& translator : orderedRelevantTranslators) {
        translation::RetTypeC2B<RetAuxType> ret =
            translator->Translate(pexpr, *this, aux);
        if (ret.success) {
          return ret;
        }
    }
    return {"NoTranslatorForOperator"_(pexpr->Pop()->SzId()), false};
  };

  translation::RetTypeC2B<RetScalarAuxType> ConvertScalar(
      CExpression* pexpr, InpScalarAuxType const& aux) {
    std::vector<ScalarTranslator*> orderedRelevantScalarTranslators =
        GetOrderedRelevantScalarTranslators(pexpr);
    for (const auto& translator : orderedRelevantScalarTranslators) {
        translation::RetTypeC2B<RetScalarAuxType> ret =
            translator->Translate(pexpr, *this, aux);
        if (ret.success) {
          return ret;
        }
    }
    return {"NoScalarTranslatorForExpression"_(pexpr->Pop()->SzId()), false};
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

  void RemoveTranslator(ULONG translatorId) override {
    translators.erase(translatorId);
  };
  void RemoveScalarTranslator(ULONG translatorId) override {
    scalarTranslators.erase(translatorId);
  };

 private:
  ULONG translatorId;
  ULONG scalarTranslatorId;

  // Store translators as unique_ptrs
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

  std::vector<Translator*> GetOrderedRelevantTranslators(
      gpopt::CExpression* pexpr) {
    std::vector<std::pair<int, Translator*>> priorityRelevantTranslators;
    for (Translator* translator : GetAllTranslators()) {
      if (translator->Match(pexpr)) {
        priorityRelevantTranslators.push_back(
            {translator->GetPriority(), translator});
      }
    }
    std::sort(priorityRelevantTranslators.begin(),
              priorityRelevantTranslators.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    std::vector<Translator*> orderedRelevantTranslators;
    for (const auto& [i, translator] : priorityRelevantTranslators) {
      orderedRelevantTranslators.push_back(translator);
    }
    return orderedRelevantTranslators;
  };

  std::vector<ScalarTranslator*> GetOrderedRelevantScalarTranslators(
      gpopt::CExpression* pexpr) {
    std::vector<std::pair<int, ScalarTranslator*>>
        priorityRelevantScalarTranslators;
    for (ScalarTranslator* translator : GetAllScalarTranslators()) {
      if (translator->Match(pexpr)) {
        priorityRelevantScalarTranslators.push_back(
            {translator->GetPriority(), translator});
      }
    }
    std::sort(priorityRelevantScalarTranslators.begin(),
              priorityRelevantScalarTranslators.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    std::vector<ScalarTranslator*> orderedRelevantScalarTranslators;
    for (const auto& [_, translator] : priorityRelevantScalarTranslators) {
      orderedRelevantScalarTranslators.push_back(translator);
    }
    return orderedRelevantScalarTranslators;
  };
};
}  // namespace cexpressiontoboss
