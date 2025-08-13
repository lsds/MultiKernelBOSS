#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformImplementation.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect: public CXformImplementation
{
private:
	// private copy ctor
	CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect(const CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect(CMemoryPool *mp);

	// ctor
	explicit CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect()
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
		return AFTransformKeys::CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbLogicalGPUSelect2PhysicalGPUSelect

}  // namespace gpopt

// EOF
