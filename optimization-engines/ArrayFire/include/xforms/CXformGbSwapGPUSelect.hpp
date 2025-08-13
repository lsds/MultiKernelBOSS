#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformExploration.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbSwapGPUSelect: public CXformExploration
{
private:
	// private copy ctor
	CXformGbSwapGPUSelect(const CXformGbSwapGPUSelect &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbSwapGPUSelect(CMemoryPool *mp);

	// ctor
	explicit CXformGbSwapGPUSelect(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbSwapGPUSelect()
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
		return AFTransformKeys::CXformGbSwapGPUSelect;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbSwapGPUSelect

}  // namespace gpopt

// EOF
