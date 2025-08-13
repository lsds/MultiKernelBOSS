#pragma once

// Forward declare CExpressionToBOSSConverter to break the circular dependency
namespace cexpressiontoboss {
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType,
          typename InpScalarAuxType>
class CExpressionToBOSSConverter;
}  // namespace cexpressiontoboss

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpression.h"  // ORCA's CExpression definition
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
// #include "gpopt/engine/CEngine.h"
// #include "gpopt/metadata/CTableDescriptor.h"
// #include "gpopt/optimizer/COptimizer.h"

#include <BOSS.hpp>  // BOSS's expression definition
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <stdexcept>
#include <unordered_set>

#include "gpdbcost/CCostModelGPDBLegacy.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CPrintPrefix.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/exception.h"
#include "gpopt/init.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CPhysicalFullMergeJoin.h"
#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizer.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"
#include "gpos/_api.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/base/IDatumInt2.h"
#include "naucrates/base/IDatumInt4.h"
#include "naucrates/base/IDatumInt8.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/init.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDProviderMemory.h"

#include "TranslatorBase.hpp"

using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Expression;
using boss::ExpressionArguments;
using boss::get;
using boss::Span;
using boss::Symbol;
using boss::expressions::CloneReason;
using boss::expressions::ExpressionSpanArgument;
using boss::expressions::ExpressionSpanArguments;

typedef CCacheAccessor<IMDCacheObject*, CMDKey*> CacheAccessorMD;

namespace cexpressiontoboss {
namespace translation {
    template <typename RetAuxType>
    struct RetTypeC2B;
}

namespace utils {
// String conversion utilities
std::wstring StringToWString(const std::string& str);
std::string WStringToString(const WCHAR* wstr);
CWStringConst* CreateWStringConst(CMemoryPool* mp, const std::string& str);

// Cardinality utilities
double GetCardinality(const CExpression* expr);

// Row size estimation
double EstimateRowSize(const CExpression* expr);

// Expression utilities
template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType,
          typename InpScalarAuxType>
translation::RetTypeC2B<RetAuxType> GetChildExprUnary(
    const CExpression* expr,
    CExpressionToBOSSConverter<RetAuxType, InpAuxType, RetScalarAuxType,
                               InpScalarAuxType>& converter,
    InpAuxType const& aux) {
  if (expr->Arity() < 1) {
    throw std::runtime_error(
        "Couldn't get expression of operator with less than 1 child");
  }
  CExpression* pexprChild = (*expr)[0];
  return converter.Convert(pexprChild, aux);
};

template <typename RetAuxType, typename InpAuxType, typename RetScalarAuxType,
          typename InpScalarAuxType>
std::pair<translation::RetTypeC2B<RetAuxType>, translation::RetTypeC2B<RetAuxType>> GetChildExprBinary(
    const CExpression* expr,
    CExpressionToBOSSConverter<RetAuxType, InpAuxType, RetScalarAuxType,
                               InpScalarAuxType>& converter,
    InpAuxType const& aux) {
  if (expr->Arity() < 2) {
    throw std::runtime_error(
        "Couldn't get expressions of operator with less than 2 children");
  }
  CExpression* pexprLeft = (*expr)[0];
  CExpression* pexprRight = (*expr)[1];
  return std::make_pair(
      converter.Convert(pexprLeft, aux),
      converter.Convert(pexprRight, aux));
};

// Date utilities
std::string FormatDate(int value);

std::unordered_set<std::string> GetOutputColumns(CExpression* expr);
std::unordered_set<std::string> GetSetIntersection(
    std::unordered_set<std::string> outputColumns,
    std::unordered_set<std::string> requiredColumns);
std::unordered_set<std::string> GetSetDifference(const std::unordered_set<std::string>& outputColumns,
                 const std::unordered_set<std::string>& requiredColumns);
Expression CreateProjectListFromColumns(
    std::unordered_set<std::string> columns);

}  // namespace utils
}  // namespace cexpressiontoboss
