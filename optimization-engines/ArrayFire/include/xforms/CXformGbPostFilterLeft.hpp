#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformExploration.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbPostFilterLeft: public CXformExploration
{
private:
	// private copy ctor
	CXformGbPostFilterLeft(const CXformGbPostFilterLeft &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbPostFilterLeft(CMemoryPool *mp);

	// ctor
	explicit CXformGbPostFilterLeft(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbPostFilterLeft()
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
		return AFTransformKeys::CXformGbPostFilterLeft;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbPostFilter

}  // namespace gpopt

// EOF
