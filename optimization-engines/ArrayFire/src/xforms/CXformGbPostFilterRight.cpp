#include "xforms/CXformGbPostFilterRight.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"
#include "utils.hpp"
#include "BOSSToCExpression.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
using namespace orcaextender;




CXformGbPostFilterRight::CXformGbPostFilterRight(CMemoryPool *mp)
	:  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
			        GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // left child join
							GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), 
								GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // child relation
								GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))), // select predicate
							GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}


CXformGbPostFilterRight::CXformGbPostFilterRight(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


CXform::EXformPromise
CXformGbPostFilterRight::Exfp(CExpressionHandle &exprhdl) const
{
	return CXform::ExfpHigh;
}


void
CXformGbPostFilterRight::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprLeftChild = (*pexpr)[0];
	CExpression *pexprJoinPred = (*pexpr)[2];
	CExpression *pexprSelect = (*pexpr)[1];
	CExpression *pexprRelational = (*pexprSelect)[0];
	CExpression *pexprSelectPred = (*pexprSelect)[1];

	// if (pexprRelational->Pop()->Eopid() == DynamicRegistry::GetInstance()->GetOperatorId(DynamicRegistry::GetInstance()->GetEngineType(AFEngineKeys::ArrayFire), AFOpKeys::CLogicalGPUSelect)) {
	// 	return;
	// }


	pexprRelational->AddRef();
	pexprLeftChild->AddRef();
	pexprJoinPred->AddRef();
	pexprSelectPred->AddRef();

	
	CExpression *pexprRes = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalSelect(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
					pexprLeftChild,
					pexprRelational,
					pexprJoinPred),
			pexprSelectPred);
  
	// add alternative to results
	pxfres->Add(pexprRes);
}




// EOF
