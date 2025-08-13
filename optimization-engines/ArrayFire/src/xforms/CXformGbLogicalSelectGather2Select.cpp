#include "xforms/CXformGbLogicalSelectGather2Select.hpp"
#include "operators/CLogicalPartialSelect.hpp"
#include "operators/logicalgather.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace orcaextender;



CXformGbLogicalSelectGather2Select::CXformGbLogicalSelectGather2Select(CMemoryPool *mp)
	:  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalPartialSelect(mp),
		 		  GPOS_NEW(mp) CExpression(
        mp, GPOS_NEW(mp) VeloxLogicalGather(mp), 
        GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))),  
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
		  ))
{
}


CXformGbLogicalSelectGather2Select::CXformGbLogicalSelectGather2Select(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


CXform::EXformPromise
CXformGbLogicalSelectGather2Select::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


void
CXformGbLogicalSelectGather2Select::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprGather = (*pexpr)[0];
  CExpression *pexprRelational = (*pexprGather)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// 1. Create the filter operator
	CExpression *pexprFilter = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalPartialSelect(mp), pexprRelational, pexprScalar);
  
	// add alternative to results
	pxfres->Add(pexprFilter);
}

// EOF
