#include "xforms/CXformGbSelect2SelectGather.hpp"
#include "operators/CLogicalPartialSelect.hpp"
#include "operators/logicalgather.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace orcaextender;


// break large select down.


CXformGbSelect2SelectGather::CXformGbSelect2SelectGather(CMemoryPool *mp)
	:  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
		  ))
{
}


CXformGbSelect2SelectGather::CXformGbSelect2SelectGather(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


CXform::EXformPromise
CXformGbSelect2SelectGather::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


void
CXformGbSelect2SelectGather::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// 1. Create the filter operator
	CExpression *pexprFilter = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalPartialSelect(mp), pexprRelational, pexprScalar);
  
	// // 2. Create the gather operator
	CExpression *pexprGather = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) VeloxLogicalGather(mp), pexprFilter);


	// add alternative to results
	pxfres->Add(pexprGather);
}

// EOF
