#include "xforms/CXformGbVeloxLogicalGather2VeloxPhysicalGather.hpp"
#include "operators/logicalgather.hpp"
#include "operators/gather.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace orcaextender;



CXformGbLogicalGather2VeloxPhysicalGather::CXformGbLogicalGather2VeloxPhysicalGather(CMemoryPool *mp)
	:  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) VeloxLogicalGather(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{}


CXformGbLogicalGather2VeloxPhysicalGather::CXformGbLogicalGather2VeloxPhysicalGather(CExpression *pexprPattern)
	: CXformImplementation(pexprPattern)
{
}


CXform::EXformPromise
CXformGbLogicalGather2VeloxPhysicalGather::Exfp(CExpressionHandle &exprhdl) const
{
	return CXform::ExfpHigh;
}


void
CXformGbLogicalGather2VeloxPhysicalGather::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
							   CExpression *pexpr) const
{

	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprChild = (*pexpr)[0];

	// addref all children
	pexprChild->AddRef();

	// 1. Create the filter operator
	CExpression *pexprGather = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CVeloxGather(mp), pexprChild);

	pxfres->Add(pexprGather);
}

// EOF
