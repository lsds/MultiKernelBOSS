#pragma once

#include "CExpressionToBOSS.hpp"
#include "c2bDefaultTypes.hpp"

namespace cexpressiontoboss {
namespace translation {
template <typename RetAuxType>
struct RetTypeC2B;
}  // namespace translation

std::string WStringToString(const WCHAR* wstr) {
  if (!wstr) {
    throw std::runtime_error("Null wide string pointer");
  }

  size_t len = wcstombs(nullptr, wstr, 0);
  if (len == static_cast<size_t>(-1)) {
    throw std::runtime_error("Failed to convert wide string to string");
  }

  std::vector<char> buffer(len + 1);
  wcstombs(buffer.data(), wstr, len + 1);
  return std::string(buffer.data());
}


std::unordered_set<std::string> GetOutputColumns(CExpression* expr) {
        std::unordered_set<std::string> requiredColumns;
        CColRefSet *pcrs = expr->DeriveOutputColumns();
        if (pcrs && pcrs->Size() > 0) {
          CColRefSetIter crsi(*pcrs);
          while (crsi.Advance()) {
            CColRef *pcr = crsi.Pcr();
            const CWStringConst *pstr = pcr->Name().Pstr();
            std::string colName = WStringToString(pstr->GetBuffer());
            requiredColumns.insert(colName);
          }
        }
        return requiredColumns;
}


class CExpressionToBOSSDefaultConverter : public CExpressionToBOSSConverter<translation::EmptyStruct, translation::ProjectInfo, translation::ColSet, translation::EmptyStruct> {
 public:
  CExpressionToBOSSDefaultConverter() = default;
  ~CExpressionToBOSSDefaultConverter() = default;

  virtual std::pair<Expression, bool> ConvertExpr(CExpression *cexpr) override {
    std::unordered_set<std::string> requiredColumns = GetOutputColumns(cexpr);

    cexpressiontoboss::translation::ProjectInfo projectInfo;
    projectInfo.parentOp = COperator::EopSentinel;
    projectInfo.requiredColumns = requiredColumns;

    translation::RetTypeC2B<translation::EmptyStruct> ret =  Convert(cexpr, projectInfo);
    return std::make_pair(std::move(ret.expr), ret.success);
  };
};
}  // namespace cexpressiontoboss
