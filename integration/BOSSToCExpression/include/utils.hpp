#pragma once

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
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/init.h"
#include "naucrates/md/CMDProviderMemory.h"

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

namespace bosstocexpression::utils {
// String conversion utilities
std::wstring StringToWString(const std::string& str);
std::string WStringToString(const WCHAR* wstr);
CWStringConst* toWString(CMemoryPool* mp, const std::string& str);
std::string toString(const WCHAR* wstr);

// Safe release functions
void safeRelease(CExpression* expr);
void safeRelease(COrderSpec* spec);
void safeRelease(CColRefArray* arr);
void safeRelease(CExpressionArray* arr);

// Efficient variadic template implementation
template <typename... Args>
void safeRelease(Args... args) {
  (safeRelease(args), ...);  // C++17 fold expression
}

std::vector<std::string> extractColumnNames(ComplexExpression&& columnListExpr);
std::unordered_map<std::string, CColRef*> CreateColumnMapping(
    CExpression* undlData);

// Date utilities
int DateToInt(std::string dateStr);

// Generate a get expression with keys from metadata
CExpression *PexprLogicalGetWithMetadataKeys(
    CMemoryPool *mp, CWStringConst *str_table_name, CWStringConst *pstrTableAlias,
    ULONG ulTableId,
    CDynamicPtrArray<CWStringConst, CleanupNULL> *pdrgpsColumnNames = NULL);

// Generate a table descriptor with keys from metadata
CTableDescriptor *PtabdescCreateWithMetadataKeys(
    CMemoryPool *mp, IMDId *mdid, const CName &nameTable,
    CDynamicPtrArray<CWStringConst, CleanupNULL>
        *pdrgpsColumnNames,  // NULL means all columns
    BOOL fPartitioned, BOOL is_nullable);

CTableDescriptor *PtabdescWithColumnNames(CMemoryPool *mp,
    IMDId *mdid, const CName &nameTable,
    CDynamicPtrArray<CWStringConst, CleanupNULL>
        *pdrgpsColumnNames,  // NULL means all columns
    BOOL is_nullable         // define nullable columns
);

// Generate a get expression with keys from metadata
CExpression *PexprLogicalGet(
    CMemoryPool *mp, CTableDescriptor *ptabdesc, const CWStringConst *pstrTableAlias);

// Generate a table descriptor with keys from metadata
CTableDescriptor *PtabdescCreateWithMetadataKeys(CMemoryPool *mp,
    IMDId *mdid, const CName &nameTable,
    CDynamicPtrArray<CWStringConst, CleanupNULL>
        *pdrgpsColumnNames,  // NULL means all columns
    BOOL fPartitioned, BOOL is_nullable);

// Creates an aggregate function expression
CExpression* CreateAggregateFunction(CMemoryPool *mp,
    const std::string& aggFuncName, CExpression* pexprInput, CColRef* colref);

// Adds an entry to an order specification
void AddOrderSpecEntry(CMemoryPool *mp,
    COrderSpec* pos, const std::string& colName,
    std::unordered_map<std::string, CColRef*> colMap, bool isAscending);

void ProcessOrderByExpression(CMemoryPool *mp,
    const boss::expressions::ComplexExpression& byExpr, COrderSpec* pos,
    const std::unordered_map<std::string, CColRef*>& colMap);

// Creates a column reference with proper name and type
CColRef *CreateColumnReference(
    CMemoryPool *mp, CMDAccessor *mda, const std::string &colName, IMDId *mdid, INT typeModifier=default_type_modifier);

// Creates a project element
CExpression* CreateProjectElement(
    CMemoryPool *mp, CMDAccessor *mda, const std::string& colName, CExpression* pexprInput);

// Creates a project list
CExpression* CreateProjectList(
    CMemoryPool *mp, CMDAccessor *mda, const std::map<std::string, CExpression*>& colExprMap);

// Extracts an as expression
bool ExtractAsExpression(ComplexExpression&& asExpr, std::map<std::string, Expression>& columnMap);

// Helper function to create binary scalar operations
CExpression* CreateBinaryScalarOp(
    CMemoryPool *mp, CExpression* pexprLeft, CExpression* pexprRight, OID mdid,
    const WCHAR* opName);

// Helper function to create unary scalar operations
CExpression* CreateUnaryScalarOp(
    CMemoryPool *mp, CExpression* pexpr, OID mdid, const WCHAR* opName);

// Helper function to create generic types
CExpression* CreateGenericType(CMemoryPool *mp, OID oid, void* val,
                                                           size_t size,
                                                           LINT lval,
                                                           CDouble dval);

std::pair<bool, Expression> ScalarComplexExpressionMatches(Expression&& bossExpr, std::vector<std::string> valid_ops);
}  // namespace bosstocexpression::utils
