#include "utils.hpp"
// #include <iostream>

// Add necessary includes for GPDB types
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CLogicalFullOuterJoin.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpos/memory/CMemoryPool.h"
#include "naucrates/base/CDatumBoolGPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/IDatum.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/base/IDatumInt4.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeInt4.h"

namespace bosstocexpression::utils {
// String conversion utilities
std::wstring StringToWString(const std::string &str) {
  size_t len = std::mbstowcs(nullptr, str.c_str(), 0);
  if (len == static_cast<size_t>(-1)) {
    std::cerr << "Failed to convert string to wide string" << std::endl;
    throw std::runtime_error("Failed to convert string to wide string");
  }

  std::vector<wchar_t> wbuffer(len + 1);
  std::mbstowcs(wbuffer.data(), str.c_str(), len + 1);
  return std::wstring(wbuffer.data());
}

std::string WStringToString(const WCHAR *wstr) {
  if (!wstr) {
    std::cerr << "Null wide string pointer" << std::endl;
    throw std::runtime_error("Null wide string pointer");
  }

  size_t len = wcstombs(nullptr, wstr, 0);
  if (len == static_cast<size_t>(-1)) {
    std::cerr << "Failed to convert wide string to string" << std::endl;
    throw std::runtime_error("Failed to convert wide string to string");
  }

  std::vector<char> buffer(len + 1);
  wcstombs(buffer.data(), wstr, len + 1);
  return std::string(buffer.data());
}

CWStringConst *toWString(CMemoryPool *mp, const std::string &str) {
  std::wstring wstr = StringToWString(str);
  return GPOS_NEW(mp) CWStringConst(wstr.c_str());
}

std::string toString(const WCHAR *wstr) { return WStringToString(wstr); }

void safeRelease(CExpression *expr) {
  if (expr) {
    expr->Release();
  }
}

void safeRelease(COrderSpec *spec) {
  if (spec) {
    spec->Release();
  }
}

void safeRelease(CColRefArray *arr) {
  if (arr) {
    arr->Release();
  }
}

void safeRelease(CExpressionArray *arr) {
  if (arr) {
    arr->Release();
  }
}

std::vector<std::string> extractColumnNames(ComplexExpression &&listExpr) {
  std::vector<std::string> columnNames;

  auto [head, ___, args, ____] = std::move(listExpr).decompose();
  if (head.getName() != "List") {
    std::cerr << "Expected List head for column names" << std::endl;
    throw std::runtime_error("Expected List head for column names");
  }

  for (const auto &arg : args) {
    if (!std::holds_alternative<Symbol>(arg)) {
      std::cerr << "Expected Symbol for column name" << std::endl;
      throw std::runtime_error("Expected Symbol for column name");
    }
    columnNames.push_back(std::get<Symbol>(arg).getName());
  }

  return columnNames;
}

// Convert column set to a map of column names to column references
std::unordered_map<std::string, CColRef *> CreateColumnMapping(
    CExpression *undlData) {
  CColRefSet *pcrs = undlData->DeriveOutputColumns();
  std::unordered_map<std::string, CColRef *> colMap;

  if (pcrs && pcrs->Size() > 0) {
    CColRefSetIter crsi(*pcrs);
    while (crsi.Advance()) {
      CColRef *pcr = crsi.Pcr();
      // Get the string from CName using Pstr() which returns CWStringConst*
      const CWStringConst *pstr = pcr->Name().Pstr();

      // Convert wide string to standard string
      std::string colName = WStringToString(pstr->GetBuffer());

      // Add to mapping
      colMap[colName] = pcr;
    }
  }

  return colMap;
}

int DateToInt(std::string dateStr) {
  int year = std::stoi(dateStr.substr(0, 4));
  int month = std::stoi(dateStr.substr(5, 2));
  int day = std::stoi(dateStr.substr(8, 2));

  return (year * 10000) + (month * 100) + day;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGetWithMetadataKeys
//
//	@doc:
//		Generate a get expression with keys from metadata
//
//---------------------------------------------------------------------------
CExpression *PexprLogicalGetWithMetadataKeys(CMemoryPool *mp,
    CWStringConst *str_table_name, CWStringConst *pstrTableAlias,
    ULONG ulTableId,
    CDynamicPtrArray<CWStringConst, CleanupNULL> *pdrgpsColumnNames) {
  CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(ulTableId, 1, 1);
  CName nameTable(str_table_name);

  // Create table descriptor with key information from metadata
  CTableDescriptor *ptabdesc =
      PtabdescCreateWithMetadataKeys(mp, mdid, nameTable, pdrgpsColumnNames,
                                     false,  // fPartitioned
                                     false   // is_nullable
      );

  CWStringConst strAlias(pstrTableAlias->GetBuffer());
  return PexprLogicalGet(mp,ptabdesc, &strAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PtabdescCreateWithMetadataKeys
//
//	@doc:
//		Generate a table descriptor with keys from metadata
//
//---------------------------------------------------------------------------
CTableDescriptor *PtabdescCreateWithMetadataKeys(CMemoryPool *mp,
    IMDId *mdid, const CName &nameTable,
    CDynamicPtrArray<CWStringConst, CleanupNULL>
        *pdrgpsColumnNames,  // NULL means all columns
    BOOL fPartitioned, BOOL is_nullable) {
  // Create table descriptor with columns from metadata
  CTableDescriptor *ptabdesc =
      PtabdescWithColumnNames(mp, mdid, nameTable, pdrgpsColumnNames, is_nullable);
  if (NULL == ptabdesc) {
    // Failed to create table descriptor
    return NULL;
  }

  // Add partition column if needed
  if (fPartitioned && ptabdesc->ColumnCount() > 0) {
    ptabdesc->AddPartitionColumn(0);
  }

  // ALL OF THIS FOR KEYS (unsure if correct)
  // Get relation metadata to access key sets
  CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
  const IMDRelation *pmdrel = md_accessor->RetrieveRel(mdid);

  // Create mapping from metadata column positions to table descriptor positions
  // This is needed because we might not include all columns from metadata in
  // the table descriptor
  CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
  const ULONG ulTotalColumns = ptabdesc->ColumnCount();

  // Map from IMDRelation col position to CTableDescriptor col position
  ULongPtrArray *pdrgpulMapping = GPOS_NEW(mp) ULongPtrArray(mp);

  // Initialize mapping with invalid values
  for (ULONG ul = 0; ul < pmdrel->ColumnCount(); ul++) {
    pdrgpulMapping->Append(GPOS_NEW(mp) ULONG(gpos::ulong_max));
  }

  // Build the mapping
  for (ULONG ulTabDescPos = 0; ulTabDescPos < ulTotalColumns; ulTabDescPos++) {
    CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ulTabDescPos];
    const CName &nameCol = pcoldesc->Name();

    // Find this column in the metadata
    for (ULONG ulMDPos = 0; ulMDPos < pmdrel->ColumnCount(); ulMDPos++) {
      const IMDColumn *pmdcol = pmdrel->GetMdCol(ulMDPos);
      if (pmdcol->IsDropped()) {
        continue;
      }

      const CWStringConst *pstrColName = pmdcol->Mdname().GetMDName();
      if (nameCol.Pstr()->Equals(pstrColName)) {
        // Found a match, update the mapping
        *((*pdrgpulMapping)[ulMDPos]) = ulTabDescPos;
        break;
      }
    }
  }

  // Get key sets from metadata and add them to table descriptor. I'm not sure
  // if this is correct.
  const ULONG ulKeySets = pmdrel->KeySetCount();
  for (ULONG ulKeySet = 0; ulKeySet < ulKeySets; ulKeySet++) {
    const ULongPtrArray *pdrgpulKeys = pmdrel->KeySetAt(ulKeySet);
    if (NULL != pdrgpulKeys && 0 < pdrgpulKeys->Size()) {
      // Create a bit set for this key
      CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp, ulTotalColumns);

      // For each key column in the metadata
      for (ULONG ulKey = 0; ulKey < pdrgpulKeys->Size(); ulKey++) {
        ULONG ulMDPos = *((*pdrgpulKeys)[ulKey]);
        // Get the corresponding position in the table descriptor
        ULONG ulTabDescPos = *((*pdrgpulMapping)[ulMDPos]);
        if (ulTabDescPos != gpos::ulong_max) {
          // This key column exists in our table descriptor
          pbs->ExchangeSet(ulTabDescPos);
        }
      }

      // Add this key set if it has at least one column
      if (pbs->Size() == 0) {
        pbs->Release();
      }
    }
  }
  // Clean up
  // Release mapping array
  pdrgpulMapping->Release();

  return ptabdesc;
}


CExpression *PexprLogicalGet(CMemoryPool *mp,
    CTableDescriptor *ptabdesc, const CWStringConst *pstrTableAlias) {
  GPOS_ASSERT(NULL != ptabdesc);

  CLogicalGet *pop = GPOS_NEW(mp)
      CLogicalGet(mp, GPOS_NEW(mp) CName(mp, CName(pstrTableAlias)), ptabdesc);

  CExpression *result = GPOS_NEW(mp) CExpression(mp, pop);

  CColRefArray *arr = pop->PdrgpcrOutput();
  for (ULONG ul = 0; ul < arr->Size(); ul++) {
    CColRef *ref = (*arr)[ul];
    ref->MarkAsUsed();
  }

  return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PtabdescWithColumnNames
//
//	@doc:
//		Generate a table descriptor with specific column names from
// table metadata
//
//---------------------------------------------------------------------------
CTableDescriptor *PtabdescWithColumnNames(CMemoryPool *mp,
    IMDId *mdid, const CName &nameTable,
    CDynamicPtrArray<CWStringConst, CleanupNULL>
        *pdrgpsColumnNames,  // NULL means all columns
    BOOL is_nullable         // define nullable columns
) {
  CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

  // Retrieve relation metadata
  const IMDRelation *pmdrel = md_accessor->RetrieveRel(mdid);

  // Create table descriptor
  CTableDescriptor *ptabdesc = GPOS_NEW(mp) CTableDescriptor(
      mp, mdid, nameTable,
      false,  // convert_hash_to_random
      IMDRelation::EreldistrRandom, IMDRelation::ErelstorageHeap,
      0  // ulExecuteAsUser
  );

  // Get all columns from the relation
  const ULONG ulAllColumns = pmdrel->ColumnCount();
  // If column names not provided, use all columns
  BOOL fUseAllColumns = (NULL == pdrgpsColumnNames);
  // Keep track of which columns in the requested list were found
  CBitSet *pbsFound = NULL;

  if (!fUseAllColumns) {
    // Initialize bit set to track found columns
    pbsFound = GPOS_NEW(mp) CBitSet(mp, pdrgpsColumnNames->Size());
  }

  // Add column descriptors from metadata
  for (ULONG ul = 0; ul < ulAllColumns; ul++) {
    const IMDColumn *pmdcol = pmdrel->GetMdCol(ul);
    if (pmdcol->IsDropped()) {
      continue;
    }

    const IMDType *pmdtype = md_accessor->RetrieveType(pmdcol->MdidType());
    const CWStringConst *pstrColName = pmdcol->Mdname().GetMDName();
    // Check if we should include this column
    BOOL fIncludeColumn = fUseAllColumns;

    if (!fUseAllColumns) {
      // Check if column is in requested list
      const ULONG ulColumns = pdrgpsColumnNames->Size();
      for (ULONG ulCol = 0; ulCol < ulColumns && !fIncludeColumn; ulCol++) {
        CWStringConst *pstrRequestedCol = (*pdrgpsColumnNames)[ulCol];
        if (pstrRequestedCol->Equals(pstrColName)) {
          fIncludeColumn = true;
          pbsFound->ExchangeSet(ulCol);
        }
      }
    }

    if (fIncludeColumn) {
      // Add this column to table descriptor
      CName nameColumn(pstrColName);
      CColumnDescriptor *pcoldesc = GPOS_NEW(mp)
          CColumnDescriptor(mp, pmdtype, pmdcol->TypeModifier(), nameColumn,
                            pmdcol->AttrNum(), is_nullable);
      ptabdesc->AddColumn(pcoldesc);
    }
  }

  // Verify that we have columns in the table descriptor and all requested
  // columns were found
  if (0 == ptabdesc->ColumnCount()) {
    ptabdesc->Release();
    GPOS_ASSERT(!"No valid columns found for table");
    return NULL;
  }

  if (!fUseAllColumns) {
    // Make sure all requested columns were found
    // Check if any bit is not set (meaning a column was not found)
    BOOL fAllColumnsFound = true;
    for (ULONG ulCol = 0; ulCol < pdrgpsColumnNames->Size(); ulCol++) {
      if (!pbsFound->Get(ulCol)) {
        fAllColumnsFound = false;
        break;
      }
    }

    if (!fAllColumnsFound) {
      ptabdesc->Release();
      GPOS_ASSERT(!"Error in scanning table: column not found");
      return NULL;
    }
    pbsFound->Release();
  }
  return ptabdesc;
}

// Creates an aggregate function expression
CExpression* CreateAggregateFunction(CMemoryPool *mp,
    const std::string& aggFuncName, CExpression* pexprInput, CColRef* colref) {
  if (!pexprInput || !colref) {
    std::cerr << "Invalid input parameters for aggregate function creation"
              << std::endl;
    throw std::runtime_error(
        "Invalid input parameters for aggregate function creation");
  }

  // Get the correct OID for the aggregate function
  CMDIdGPDB* pmdidAggFunc = nullptr;
  if (aggFuncName == "Sum") {
    pmdidAggFunc = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_AGG_SUM);
  } else if (aggFuncName == "Count") {
    pmdidAggFunc = GPOS_NEW(mp) CMDIdGPDB(GPDB_COUNT_ANY);
  } else if (aggFuncName == "Min") {
    pmdidAggFunc = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_AGG_MIN);
  } else if (aggFuncName == "Max") {
    pmdidAggFunc = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_AGG_MAX);
  } else if (aggFuncName == "Avg") {
    pmdidAggFunc = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_AGG_AVG);
  } else {
    std::cerr << "Unsupported aggregate function: " << aggFuncName << std::endl;
    throw std::runtime_error("Unsupported aggregate function: " + aggFuncName);
  }

  std::wstring waggFuncName = utils::StringToWString(aggFuncName);
  CWStringConst* strAggFunc =
      GPOS_NEW(mp) CWStringConst(mp, waggFuncName.c_str());

  CScalarAggFunc* popScAggFunc = GPOS_NEW(mp) CScalarAggFunc(
      mp, pmdidAggFunc, nullptr, strAggFunc, false, EaggfuncstageGlobal, false);

  // Create the aggregate expression
  CExpressionArray* pdrgpexprAggInput = GPOS_NEW(mp) CExpressionArray(mp);
  pdrgpexprAggInput->Append(pexprInput);
  CExpression* pexprAgg =
      GPOS_NEW(mp) CExpression(mp, popScAggFunc, pdrgpexprAggInput);

  // Create project element
  return CUtils::PexprScalarProjectElement(mp, colref, pexprAgg);
}

// Adds an entry to an order specification
void addOrderSpecEntry(CMemoryPool *mp,
    COrderSpec* pos, const std::string& colName,
    std::unordered_map<std::string, CColRef*> colMap, bool isAscending) {
  CColRef* colref;
  if (colMap.find(colName) != colMap.end()) {
    colref = colMap[colName];
  } else {
    std::cerr << "Order by column not found in column mapping: " << colName
              << std::endl;
    throw;
  }

  // - NULLs FIRST for DESC, NULLs LAST for ASC
  COrderSpec::ENullTreatment nullTreatment =
      isAscending ? COrderSpec::EntLast : COrderSpec::EntFirst;

  // Add to order spec with appropriate direction
  if (isAscending) {
    pos->Append(GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_LT_OP), colref, nullTreatment);
  } else {
    pos->Append(GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_GT_OP), colref, nullTreatment);
  }
}

void ProcessOrderByExpression(CMemoryPool *mp,
    const boss::expressions::ComplexExpression& byExpr, COrderSpec* pos,
    const std::unordered_map<std::string, CColRef*>& colMap) {
  if (byExpr.getHead().getName() == "By") {
    // Process the order columns in the By expression
    // Each pair of arguments represents (column_name, direction)
    const auto& args = byExpr.getArguments();

    if (args.size() % 2 != 0) {
      std::cerr << "Each column in 'By' must have a direction. Format: "
                   "By(col1, dir1, col2, dir2, ...)";
      throw std::runtime_error(
          "Each column in 'By' must have a direction. Format: By(col1, dir1, "
          "col2, dir2, ...)");
    }

    for (size_t i = 0; i < args.size(); i += 2) {
      // First argument should be the column name
      if (!boss::expressions::generic::holds_alternative<
              boss::expressions::Symbol>(args[i])) {
        std::cerr << "Expected Symbol for column name in 'By'" << std::endl;
        throw std::runtime_error("Expected Symbol for column name in 'By'");
      }

      // Second argument should be the direction ("asc"_ or "desc"_)
      if (!boss::expressions::generic::holds_alternative<
              boss::expressions::Symbol>(args[i + 1])) {
        std::cerr << "Expected Symbol for direction ('asc' or 'desc') in 'By'"
                  << std::endl;
        throw std::runtime_error(
            "Expected Symbol for direction ('asc' or 'desc') in 'By'");
      }

      // Extract column name
      const auto& colSymbol =
          boss::expressions::generic::get<boss::expressions::Symbol>(args[i]);
      std::string colName = colSymbol.getName();

      // Extract direction
      const auto& dirSymbol =
          boss::expressions::generic::get<boss::expressions::Symbol>(
              args[i + 1]);
      std::string direction = dirSymbol.getName();

      // Validate direction
      if (direction != "asc" && direction != "desc") {
        std::cerr << "Direction in 'By' must be either 'asc' or 'desc'"
                  << std::endl;
        throw std::runtime_error(
            "Direction in 'By' must be either 'asc' or 'desc'");
      }

      // Add to the order spec
      bool isAscending = (direction == "asc");
      addOrderSpecEntry(mp, pos, colName, colMap, isAscending);
    }
  } else {
    std::cerr << "Expected 'By' head for ordering specification" << std::endl;
    throw std::runtime_error("Expected 'By' head for ordering specification");
  }
}

CExpression* CreateProjectElement(
    CMemoryPool *mp, CMDAccessor *mda, const std::string& colName, CExpression* pexprInput) {
  if (!pexprInput) {
    std::cerr << "Invalid input expression for project element" << std::endl;
    throw std::runtime_error("Invalid input expression for project element");
  }

  // Get the type metadata ID from the input expression
  IMDId* mdid = nullptr;
  COperator* pop = pexprInput->Pop();

  // Only scalar operators have a MdidType method
  if (pop->FScalar()) {
    try {
      mdid = CScalar::PopConvert(pop)->MdidType();
      if (mdid && mdid->IsValid()) {
        mdid->AddRef();
      }
    } catch (const std::exception& e) {
      std::cerr << "Failed to get type metadata ID from input expression: "
                << e.what() << std::endl;
      throw std::runtime_error(
          "Failed to get type metadata ID from input expression: " +
          std::string(e.what()));
    }
  }

  CColRef* colref = CreateColumnReference(mp, mda, colName, mdid);

  if (!colref) {
    utils::safeRelease(pexprInput);
    std::cerr << "Failed to create column reference for project element: "
              << colName << std::endl;
    throw std::runtime_error(
        "Failed to create column reference for project element: " + colName);
  }

  return GPOS_NEW(mp) CExpression(
      mp, GPOS_NEW(mp) CScalarProjectElement(mp, colref), pexprInput);
}

CExpression* CreateProjectList(
    CMemoryPool *mp, CMDAccessor *mda, const std::map<std::string, CExpression*>& colExprMap) {
  CExpressionArray* pdrgpexprProjectElements =
      GPOS_NEW(mp) CExpressionArray(mp);

  for (const auto& [colName, expr] : colExprMap) {
    CExpression* projElem = CreateProjectElement(mp, mda, colName, expr);
    if (projElem) {
      pdrgpexprProjectElements->Append(projElem);
    }
  }

  if (pdrgpexprProjectElements->Size() == 0) {
    utils::safeRelease(pdrgpexprProjectElements);
    std::cerr << "No valid project elements found" << std::endl;
    throw std::runtime_error("No valid project elements found");
  }

  return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
                                  pdrgpexprProjectElements);
}

bool ExtractAsExpression(ComplexExpression&& asExpr, std::map<std::string, Expression>& columnMap) {
  auto [asHead, ___, asArgs, ____] = std::move(asExpr).decompose();

  // Validate that this is an 'as' expression with at least 2 arguments
  if (asHead.getName() != "As" || asArgs.size() < 2 || asArgs.size() % 2 != 0) {
    std::cerr << "Invalid 'As' expression: must have an even number of "
                 "arguments (column name, expression pairs)"
              << std::endl;
    throw std::runtime_error(
        "Invalid 'As' expression: must have an even number of arguments "
        "(column name, expression pairs)");
  }

  // Process pairs of arguments
  for (size_t i = 0; i < asArgs.size(); i += 2) {
    // The first argument in each pair must be a symbol representing the output
    // column name
    if (!std::holds_alternative<Symbol>(asArgs[i])) {
      std::cerr << "Column name in 'As' expression must be a symbol"
                << std::endl;
      throw std::runtime_error(
          "Column name in 'As' expression must be a symbol");
    }

    std::string columnName = std::get<Symbol>(asArgs[i]).getName();
    Expression valueExpr = std::move(asArgs[i + 1]);

    columnMap[columnName] = std::move(valueExpr);
  }

  return !columnMap.empty();
}

// Creates a column reference with proper name and type
CColRef *CreateColumnReference(
    CMemoryPool *mp, CMDAccessor *mda, const std::string &colName, IMDId *mdid, INT typeModifier) {
  if (colName.empty()) {
    std::cerr << "Column name cannot be empty" << std::endl;
    throw std::runtime_error("Column name cannot be empty");
  }

  std::wstring wcolName = utils::StringToWString(colName);
  CWStringConst strColName(wcolName.c_str());
  CName name(mp, &strColName);

  if (!mdid) {
    mdid = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_OID);
  }

  const IMDType *pmdtype = mda->RetrieveType(mdid);
  mdid->Release();
  return COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(pmdtype, typeModifier,
                                                     name);
}


// Helper function to create binary scalar operations
CExpression* CreateBinaryScalarOp(
    CMemoryPool *mp, CExpression* pexprLeft, CExpression* pexprRight, OID mdid,
    const WCHAR* opName) {
  if (!pexprLeft || !pexprRight) {
    utils::safeRelease(pexprLeft, pexprRight);
    std::cerr << "Failed to create binary operation operands" << std::endl;
    throw std::runtime_error("Failed to create binary operation operands");
  }

  // Create the operator ID
  IMDId* pmdidOp = GPOS_NEW(mp) CMDIdGPDB(mdid);

  // Create the return type mdid (using INT4 for now). TODO get from metadata?
  IMDId* pmdidReturnType = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_OID);

  // Create the scalar operation
  CScalarOp* popScalarOp = GPOS_NEW(mp) CScalarOp(
      mp, pmdidOp, pmdidReturnType, GPOS_NEW(mp) CWStringConst(opName));

  // Create the expression
  return GPOS_NEW(mp) CExpression(mp, popScalarOp, pexprLeft, pexprRight);
}

// Helper function to create unary scalar operations
CExpression* CreateUnaryScalarOp(
    CMemoryPool *mp, CExpression* pexpr, OID mdid, const WCHAR* opName) {
  if (!pexpr) {
    utils::safeRelease(pexpr);
    std::cerr << "Failed to create unary operation operand" << std::endl;
    throw std::runtime_error("Failed to create unary operation operand");
  }

  // Create the operator ID
  IMDId* pmdidOp = GPOS_NEW(mp) CMDIdGPDB(mdid);

  // Create the return type mdid (using INT4 for now). 
  IMDId* pmdidReturnType = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_OID);

  // Create the scalar operation
  CScalarOp* popScalarOp = GPOS_NEW(mp) CScalarOp(
      mp, pmdidOp, pmdidReturnType, GPOS_NEW(mp) CWStringConst(opName));

  // Create the expression
  return GPOS_NEW(mp) CExpression(mp, popScalarOp, pexpr);
}

CExpression* CreateGenericType(CMemoryPool *mp, OID oid, void* val,
                                                           size_t size,
                                                           LINT lval,
                                                           CDouble dval) {
  // Create a double constant using CDatumGenericGPDB
  IMDId* mdid = GPOS_NEW(mp) CMDIdGPDB(oid);
  CDatumGenericGPDB* datum = GPOS_NEW(mp)
      CDatumGenericGPDB(mp, mdid,
                        -1,  // Type modifier
                        val, size,
                        false,  // Not null
                        lval,   // Integer approximation for statistics
                        dval    // Double value for statistics
      );

  CScalarConst* popScalarConst = GPOS_NEW(mp) CScalarConst(mp, datum);
  return GPOS_NEW(mp) CExpression(mp, popScalarConst);
}


std::pair<bool, Expression> ScalarComplexExpressionMatches(Expression &&bossExpr, std::vector<std::string> valid_ops) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> std::pair<bool, Expression> {
            auto [head, args1, args2, args3] = std::move(cexpr).decompose();
            if (std::find(valid_ops.begin(), valid_ops.end(), head.getName()) != valid_ops.end()) {
              return std::make_pair(true, ComplexExpression{std::move(head), std::move(args1), std::move(args2), std::move(args3)});
            }
            return std::make_pair(false, ComplexExpression{std::move(head), std::move(args1), std::move(args2), std::move(args3)});
          },
          [&](auto &&val) -> std::pair<bool, Expression> { return std::make_pair(false, std::move(val)); }),
      std::move(bossExpr));
}

}  // namespace bosstocexpression::utils
