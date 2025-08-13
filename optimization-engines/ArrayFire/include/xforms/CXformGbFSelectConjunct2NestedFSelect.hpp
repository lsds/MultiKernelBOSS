#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformExploration.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbFSelectConjunct2NestedFSelect: public CXformExploration
{
private:
	// private copy ctor
	CXformGbFSelectConjunct2NestedFSelect(const CXformGbFSelectConjunct2NestedFSelect &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbFSelectConjunct2NestedFSelect(CMemoryPool *mp);

	// ctor
	explicit CXformGbFSelectConjunct2NestedFSelect(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbFSelectConjunct2NestedFSelect()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return DynamicRegistry::GetInstance()->GetTransformId(SzId());
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return AFTransformKeys::CXformGbFSelectConjunct2NestedFSelect;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbFSelectConjunct2NestedFSelect

}  // namespace gpopt

// EOF
