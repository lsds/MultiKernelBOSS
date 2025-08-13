#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformExploration.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;

class CXformGbPostFilterRight: public CXformExploration
{
private:
	// private copy ctor
	CXformGbPostFilterRight(const CXformGbPostFilterRight &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbPostFilterRight(CMemoryPool *mp);

	// ctor
	explicit CXformGbPostFilterRight(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbPostFilterRight()
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
		return AFTransformKeys::CXformGbPostFilterRight;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbPostFilterRight

}  // namespace gpopt

// EOF
