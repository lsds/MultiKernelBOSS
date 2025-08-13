
#include "xforms/CXformGbSwapGPUSelect.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"

using namespace orcaextender;

// order selects by selectivity.

CXformGbSwapGPUSelect::CXformGbSwapGPUSelect(CMemoryPool *mp)
	:  CXformExploration(
      GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
		      GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp),
			      GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),
                  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))), 
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)))) 
{
}


CXformGbSwapGPUSelect::CXformGbSwapGPUSelect(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


CXform::EXformPromise
CXformGbSwapGPUSelect::Exfp(CExpressionHandle &exprhdl) const
{
    // could use cost context here actually. JUST USE STATISTICS.
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


void
CXformGbSwapGPUSelect::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                           CExpression *pexpr) const
{
    GPOS_ASSERT(NULL != pxfctxt);
    GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
    GPOS_ASSERT(FCheckPattern(pexpr));

    CMemoryPool *mp = pxfctxt->Pmp();

    // extract components
    CExpression *pexprNestedSelect = (*pexpr)[0];
    CExpression *pexprScalar = (*pexpr)[1];

    CExpression *pexprRelational = (*pexprNestedSelect)[0];
    CExpression *pexprScalar2 = (*pexprNestedSelect)[1];

    const IStatistics *stats = pexpr->Pstats();
    const IStatistics *childStats = pexprScalar->Pstats();
    const IStatistics *grandchildStats = pexprScalar2->Pstats();

    if (stats == NULL || childStats == NULL || grandchildStats == NULL) {
        return;
    }

    CDouble rows = pexpr->Pstats()->Rows();
    CDouble rows_child = pexprScalar->Pstats()->Rows();
    CDouble rows_grandchild = pexprScalar2->Pstats()->Rows();

    if ((rows / rows_child) < (rows_child / rows_grandchild)) {
        return;
    }

    pexprRelational->AddRef();
    pexprScalar->AddRef();
    pexprScalar2->AddRef();

    CExpression *finalSelect = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), 
                                                                GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), 
                                                                    pexprRelational, 
                                                                    pexprScalar), 
                                                                pexprScalar2);

    pxfres->Add(finalSelect);
}

// EOF
