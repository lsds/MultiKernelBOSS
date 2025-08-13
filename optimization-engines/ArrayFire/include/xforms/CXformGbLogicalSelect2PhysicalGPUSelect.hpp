#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformImplementation.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbLogicalSelect2PhysicalGPUSelect: public CXformImplementation
{
private:
	// private copy ctor
	CXformGbLogicalSelect2PhysicalGPUSelect(const CXformGbLogicalSelect2PhysicalGPUSelect &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbLogicalSelect2PhysicalGPUSelect(CMemoryPool *mp);

	// ctor
	explicit CXformGbLogicalSelect2PhysicalGPUSelect(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbLogicalSelect2PhysicalGPUSelect()
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
		return AFTransformKeys::CXformGbLogicalSelect2PhysicalGPUSelect;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbLogicalGPUSelect2PhysicalGPUSelect

}  // namespace gpopt

// EOF
