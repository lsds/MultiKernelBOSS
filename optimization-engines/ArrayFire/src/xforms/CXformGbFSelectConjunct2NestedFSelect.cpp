#include "xforms/CXformGbFSelectConjunct2NestedFSelect.hpp"
#include "operators/CLogicalPartialSelect.hpp"
#include "gpoptextender/EngineProperty/CPhysicalEngineTransition.hpp"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"

using namespace orcaextender;



CXformGbFSelectConjunct2NestedFSelect::CXformGbFSelectConjunct2NestedFSelect(CMemoryPool *mp)
	:  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternTree(mp)),  // relational child
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // Child of AND (expression array)
          )
      )
{
}


CXformGbFSelectConjunct2NestedFSelect::CXformGbFSelectConjunct2NestedFSelect(CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


CXform::EXformPromise
CXformGbFSelectConjunct2NestedFSelect::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


void
CXformGbFSelectConjunct2NestedFSelect::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                           CExpression *pexpr) const
{
    GPOS_ASSERT(NULL != pxfctxt);
    GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
    GPOS_ASSERT(FCheckPattern(pexpr));


    CMemoryPool *mp = pxfctxt->Pmp();

    // extract components
    CExpression *pexprRelational = (*pexpr)[0];
    CExpression *pexprScalar = (*pexpr)[1];

    if (pexprScalar->Pop()->Eopid() != COperator::EopScalarBoolOp || CScalarBoolOp::PopConvert(pexprScalar->Pop())->Eboolop() != CScalarBoolOp::EboolopAnd) {
      return;
    }

    pexprRelational->AddRef();
    CExpression *pexprCurrent = pexprRelational;
    std::unordered_map<const CColRef *, std::vector<CExpression *>> colRefExpressions;
    std::vector<CExpression *> nonColRefExpressions;
    
    for (ULONG ul = 0; ul < pexprScalar->Arity(); ul++) {
      CExpression *pexprPred = (*pexprScalar)[ul];
      pexprPred->AddRef();

      if (pexprPred->Pop()->Eopid() == COperator::EopScalarCmp && pexprPred->Arity() == 2) {
        CExpression *child1 = (*pexprPred)[0];
        CExpression *child2 = (*pexprPred)[1];

        if (child1->Pop()->Eopid() == COperator::EopScalarIdent && child2->Pop()->Eopid() != COperator::EopScalarIdent) {
          const CColRef *colRef = CScalarIdent::PopConvert(child1->Pop())->Pcr();
          colRefExpressions[colRef].push_back(pexprPred);
          continue;
        }

        if (child2->Pop()->Eopid() == COperator::EopScalarIdent && child1->Pop()->Eopid() != COperator::EopScalarIdent) {
          const CColRef *colRef = CScalarIdent::PopConvert(child2->Pop())->Pcr();
          colRefExpressions[colRef].push_back(pexprPred);
          continue;
        }
      }

      nonColRefExpressions.push_back(pexprPred);
    }

    if (nonColRefExpressions.size() == 0 && colRefExpressions.size() == 1) {
      return;
    }

    if (nonColRefExpressions.size() == 1 && colRefExpressions.size() == 0) {
      return;
    }

    for (const auto& [_, e_list] : colRefExpressions) {
      if (e_list.size() == 1) {
        pexprCurrent = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), pexprCurrent, e_list[0]);
      } else {
        CExpressionArray* pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
        for (CExpression *e : e_list) {
          pdrgpexpr->Append(e);
        }

        CExpression *conjunct = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarBoolOp(mp, CScalarBoolOp::EboolopAnd), pdrgpexpr);

        pexprCurrent = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalPartialSelect(mp), pexprCurrent, conjunct);
      }
    }

    for (CExpression *e : nonColRefExpressions) {
      pexprCurrent = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalSelect(mp), pexprCurrent, e);
    }

    pxfres->Add(pexprCurrent);
}

// EOF
