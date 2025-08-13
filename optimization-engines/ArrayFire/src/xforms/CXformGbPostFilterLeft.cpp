#include "xforms/CXformGbPostFilterLeft.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace orcaextender;




CXformGbPostFilterLeft::CXformGbPostFilterLeft(CMemoryPool *mp)
	:  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
							GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), 
								GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // child relation
								GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))), // select predicate
        			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // right child join
							GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}


CXformGbPostFilterLeft::CXformGbPostFilterLeft(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


CXform::EXformPromise
CXformGbPostFilterLeft::Exfp(CExpressionHandle &exprhdl) const
{
	// TODO: check if pushing the selection out is legal (e.g. the column is not on the other side).
	return CXform::ExfpHigh;
}


void
CXformGbPostFilterLeft::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRightChild = (*pexpr)[1];
	CExpression *pexprJoinPred = (*pexpr)[2];
	CExpression *pexprSelect = (*pexpr)[0];
	CExpression *pexprRelational = (*pexprSelect)[0];
	CExpression *pexprSelectPred = (*pexprSelect)[1];

	// if (pexprRelational->Pop()->Eopid() == DynamicRegistry::GetInstance()->GetOperatorId(DynamicRegistry::GetInstance()->GetEngineType(AFEngineKeys::ArrayFire), AFOpKeys::CLogicalGPUSelect)) {
	// 	return;
	// }

	pexprRelational->AddRef();
	pexprRightChild->AddRef();
	pexprJoinPred->AddRef();
	pexprSelectPred->AddRef();

	
	CExpression *pexprRes = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalSelect(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
					pexprRelational,
					pexprRightChild,
					pexprJoinPred),
			pexprSelectPred);
  
	// add alternative to results
	pxfres->Add(pexprRes);
}

// EOF
