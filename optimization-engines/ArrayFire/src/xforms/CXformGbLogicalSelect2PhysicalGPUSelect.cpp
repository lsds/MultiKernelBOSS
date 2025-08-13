#include "xforms/CXformGbLogicalSelect2PhysicalGPUSelect.hpp"
#include "operators/CPhysicalGPUFullSelect.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace orcaextender;



CXformGbLogicalSelect2PhysicalGPUSelect::CXformGbLogicalSelect2PhysicalGPUSelect(CMemoryPool *mp)
	:  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
		  ))
{
}


CXformGbLogicalSelect2PhysicalGPUSelect::CXformGbLogicalSelect2PhysicalGPUSelect(CExpression *pexprPattern)
	: CXformImplementation(pexprPattern)
{
}


CXform::EXformPromise
CXformGbLogicalSelect2PhysicalGPUSelect::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


void
CXformGbLogicalSelect2PhysicalGPUSelect::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
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
		mp, GPOS_NEW(mp) CPhysicalGPUFullSelect(mp), pexprRelational, pexprScalar);

	pxfres->Add(pexprFilter);
}

// EOF
