#pragma once
#include "ScalarTranslatorBase.hpp"
#include "Translator.hpp"

namespace bosstocexpression {
// NOTE THESE ARE METADATA DEPENDENT. MAKE SURE METADATA DEFINES THESE OIDS FOR
// THESE OPS
#define GPDB_INT4_OID OID(23)
#define GPDB_INT4_ADD_OP OID(551)
#define GPDB_INT4_SUB_OP OID(552)
#define GPDB_INT4_MUL_OP OID(594)
#define GPDB_INT4_DIV_OP OID(691)
#define GPDB_INT4_YEAR_OP OID(1380)
#define GPDB_INT4_STRINGCONTAINSQ_OP OID(1381)
#define GPDB_TEXT_OID OID(25)       // Text type OID
#define GPDB_VARCHAR_OID OID(1043)  // VARCHAR type OID
#define GPDB_FLOAT4_OID OID(700)    // Float4 type OID
#define GPDB_FLOAT8_OID OID(701)    // Float8 (double) type OID
#define GPDB_NUMERIC_OID OID(1700)  // Numeric type OID
#define GPDB_DATE_OID OID(1082)     // Date type OID

class ScalarTranslator : public ScalarTranslatorBase<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> {
  public:
    ScalarTranslator(CMemoryPool *mp, CMDAccessor *mda) : mp(mp), mda(mda) {}
    virtual ~ScalarTranslator() = default;

    virtual std::pair<bool, Expression> Match(Expression &&bossExpr) override = 0;
    virtual RetType<EmptyStruct> Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) override = 0;
    virtual int GetPriority() override = 0;

  protected:
    CMemoryPool *mp;
    CMDAccessor *mda;
};

}  // namespace bosstocexpression