#include "translation/scalar/CScalarConstToBOSSScalarConst.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CScalarConstToBOSSScalarConst::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarConst;
}

RetTypeC2B<ColSet> CScalarConstToBOSSScalarConst::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  CScalarConst* scalarConst = CScalarConst::PopConvert(expr->Pop());
  IDatum* datum = scalarConst->GetDatum();

  if (!datum) {
    throw std::runtime_error("Scalar constant has no datum");
  }

  // Handle different constant types
  switch (datum->GetDatumType()) {
    case IMDType::EtiInt2: {
      IDatumInt2* int2Datum = dynamic_cast<IDatumInt2*>(datum);
      if (!int2Datum) {
        throw std::runtime_error("Failed to cast to INT2 datum");
      }
      return {int2Datum->Value(), success, requiredColumns};
    }
    case IMDType::EtiInt4: {
      IDatumInt4* int4Datum = dynamic_cast<IDatumInt4*>(datum);
      if (!int4Datum) {
        throw std::runtime_error("Failed to cast to INT4 datum");
      }

      int val = int4Datum->Value(); 
      orcaextender::DynamicRegistry *dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
      auto ptr = std::any_cast<std::shared_ptr<std::unordered_map<int, std::string>>>(dynamicRegistry->GetAuxiliaryState("intToStringMapping"));
      if (ptr->find(val) != ptr->end()) {
        return {(*ptr)[val], success, requiredColumns};
      }

      return {int4Datum->Value(), success, requiredColumns};
    }
    case IMDType::EtiInt8: {
      IDatumInt8* int8Datum = dynamic_cast<IDatumInt8*>(datum);
      if (!int8Datum) {
        throw std::runtime_error("Failed to cast to INT8 datum");
      }
      return {int8Datum->Value(), success, requiredColumns};
    }
    case IMDType::EtiBool: {
      IDatumBool* boolDatum = dynamic_cast<IDatumBool*>(datum);
      if (!boolDatum) {
        throw std::runtime_error("Failed to cast to BOOL datum");
      }
      return {boolDatum->GetValue(), success, requiredColumns};
    }
    case IMDType::EtiGeneric: {
      // For EtiGeneric, try to determine the actual data type from the OID
      CDatumGenericGPDB* genericDatum = dynamic_cast<CDatumGenericGPDB*>(datum);
      if (!genericDatum) {
        throw std::runtime_error("Failed to cast to generic GPDB datum");
      }

      // Get the OID of the datum
      IMDId* mdid = genericDatum->MDId();
      if (genericDatum->IsNull()) {
        return {"NULL"_, success, requiredColumns};
      }

      const BYTE* byteArray = genericDatum->GetByteArrayValue();
      ULONG size = genericDatum->Size();

      // Check for TEXT (OID 25) or VARCHAR (OID 1043)
      if (mdid->Equals(&CMDIdGPDB::m_mdid_text) ||     // TEXT (OID 25)
          mdid->Equals(&CMDIdGPDB::m_mdid_varchar)) {  // VARCHAR (OID 1043)

        if (!byteArray || size == 0) {
          return {std::string(""), success, requiredColumns};
        }

        return {std::string(reinterpret_cast<const char*>(byteArray), size), success, requiredColumns};
      }
      // Check for FLOAT4 (OID 700)
      else if (mdid->Equals(&CMDIdGPDB::m_mdid_float4)) {
        float value = 0.0f;
        if (byteArray && size >= sizeof(float)) {
          // Retrieve the float value from the byte array
          return {*reinterpret_cast<const float*>(byteArray), success, requiredColumns};
        }
        throw std::runtime_error("Not a float");
      }
      // Check for FLOAT8/DOUBLE (OID 701)
      else if (mdid->Equals(&CMDIdGPDB::m_mdid_float8)) {
        double value = 0.0;
        if (byteArray && size >= sizeof(double)) {
          // Retrieve the double value from the byte array
          return {*reinterpret_cast<const double*>(byteArray), success, requiredColumns};
        }
        throw std::runtime_error("Not a double");
      } else if (mdid->Equals(&CMDIdGPDB::m_mdid_date)) {
        int value = 0;
        if (byteArray && size >= sizeof(int)) {
          // Retrieve the int value from the byte array
          value = *reinterpret_cast<const int*>(byteArray);
          return {"DateObject"_(utils::FormatDate(value)), success, requiredColumns};
        }
        throw std::runtime_error("Not a date");
      }
      throw std::runtime_error("Unsupported constant type");

      // Fall through to default handling for other generic types
    }
    default:
      throw std::runtime_error("Unsupported constant type");
  }
}

int CScalarConstToBOSSScalarConst::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation