#include "xforms/CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect.hpp"
#include "operators/CLogicalPartialSelect.hpp"
#include "operators/CPhysicalGPUPartialSelect.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace orcaextender;


CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect::CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect(CMemoryPool *mp)
	:  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalPartialSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
		  ))
{
}

CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect::CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect(CExpression *pexprPattern)
	: CXformImplementation(pexprPattern)
{
}

//---------------------------------------------------------------------------

CXform::EXformPromise
CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


void
CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
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
		mp, GPOS_NEW(mp) CPhysicalGPUPartialSelect(mp), pexprRelational, pexprScalar);

	pxfres->Add(pexprFilter);
}

// EOF
