#include "xforms/CXformLogicalJoin2PhysicalGPUJoin.hpp"

#include "gpos/base.h"

#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

#include "operators/CPhysicalGPUJoin.hpp"

using namespace orcaextender;



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


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoin2HashJoin::CXformInnerJoin2HashJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLogicalJoin2PhysicalGPUJoin::CXformLogicalJoin2PhysicalGPUJoin(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}


CXform::EXformPromise
CXformLogicalJoin2PhysicalGPUJoin::Exfp(CExpressionHandle &exprhdl) const
{
	CExpression *pexpr = exprhdl.PexprScalarExactChild(2);
	CColRefSet *innerCols = exprhdl.DeriveOutputColumns(1);
	
	if (pexpr->Pop()->Eopid() == COperator::EopScalarCmp) {
		if (CScalarCmp::PopConvert(pexpr->Pop())->ParseCmpType() == IMDType::EcmptEq) {
			CExpression *pexprLeftChild = (*pexpr)[0];
			CExpression *pexprRightChild = (*pexpr)[1];
			if (pexprLeftChild->Pop()->Eopid() == COperator::EopScalarIdent && pexprRightChild->Pop()->Eopid() == COperator::EopScalarIdent) {
				const CColRef *leftColRef = CScalarIdent::PopConvert(pexprLeftChild->Pop())->Pcr();
				const CColRef *rightColRef = CScalarIdent::PopConvert(pexprRightChild->Pop())->Pcr();
				const CColRef *primaryColRef;

				// hacky - TODO find a way to get primary key from a column - Maybe register via dynamic registry.
				std::unordered_set<std::string> singlePks = {"p_partkey", "c_custkey", "o_orderkey", "n_nationkey", "r_regionkey", "s_suppkey"};
				std::string leftColName = WStringToString(leftColRef->Name().Pstr()->GetBuffer());
				std::string rightColName = WStringToString(rightColRef->Name().Pstr()->GetBuffer());

				if (singlePks.find(leftColName) != singlePks.end()) {
					primaryColRef = leftColRef;
				} else if (singlePks.find(rightColName) != singlePks.end()) {
					primaryColRef = rightColRef;
				} else {
					return CXform::ExfpNone;
				}

				if (innerCols->FMember(primaryColRef)) {
					return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
				}
			}
		}
	}

	return CXform::ExfpNone;
}


BOOL FilterPresent(CExpression* expr) {
	DynamicRegistry *dynamicRegistry = DynamicRegistry::GetInstance();
	EEngineType engineType = dynamicRegistry->GetEngineType(AFEngineKeys::ArrayFire);
	COperator::EOperatorId opId = dynamicRegistry->GetOperatorId(engineType, AFOpKeys::CLogicalPartialSelect);
	if (expr->Pop()->Eopid() == COperator::EopLogicalSelect || expr->Pop()->Eopid() == opId) {
		return true;
	}

	if (expr->Pop()->FScalar()) {
		return false;
	}

	// Recursively check all children
  const ULONG arity = expr->Arity();
  for (ULONG i = 0; i < arity; ++i) {
    if (FilterPresent((*expr)[i])) {
      return true;
		}
  }


  return false;
}



void
CXformLogicalJoin2PhysicalGPUJoin::Transform(CXformContext *pxfctxt,
									CXformResult *pxfres,
									CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));
	
	if (!FilterPresent((*pexpr)[1])) {
		CXformUtils::ImplementHashJoin<CPhysicalGPUJoin>(pxfctxt, pxfres,
														   pexpr, Exfid());
	}

}

// EOF
