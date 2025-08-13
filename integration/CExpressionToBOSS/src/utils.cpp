#include "utils.hpp"

#include "CExpressionToBOSS.hpp"

#include <iomanip>  // For std::setw, std::setfill
#include <sstream>  // For std::stringstream

namespace cexpressiontoboss::utils {

// String conversion utilities

// Convert a std::string to a wide string
std::wstring StringToWString(const std::string& str) {
  size_t len = std::mbstowcs(nullptr, str.c_str(), 0);
  if (len == static_cast<size_t>(-1)) {
    throw std::runtime_error("Failed to convert string to wide string");
  }

  std::vector<wchar_t> wbuffer(len + 1);
  std::mbstowcs(wbuffer.data(), str.c_str(), len + 1);
  return std::wstring(wbuffer.data());
}

// Convert a wide string to a std::string
std::string WStringToString(const WCHAR* wstr) {
  if (!wstr) {
    throw std::runtime_error("Null wide string pointer");
  }

  size_t len = wcstombs(nullptr, wstr, 0);
  if (len == static_cast<size_t>(-1)) {
    throw std::runtime_error("Failed to convert wide string to string");
  }

  std::vector<char> buffer(len + 1);
  wcstombs(buffer.data(), wstr, len + 1);
  return std::string(buffer.data());
}

// Create a CWStringConst from a std::string
CWStringConst* CreateWStringConst(CMemoryPool* mp, const std::string& str) {
  std::wstring wstr = StringToWString(str);
  return GPOS_NEW(mp) CWStringConst(wstr.c_str());
}



// Get the cardinality estimate (number of rows) for an expression
double GetCardinality(const CExpression* expr) {
  // Get stats from the expression
  const IStatistics* stats = expr->Pstats();
  if (stats == nullptr) {
    // Default to 1 if no statistics are available
    return 1.0;
  }

  return stats->Rows().Get();
}

// Estimate the average row size in bytes for an expression's output
double EstimateRowSize(const CExpression* expr) {
  if (expr == nullptr) {
    throw std::runtime_error("EstimateRowSize: Expression is nullptr");
  }

  // Try to get output columns from the expression's derived properties
  // The const_cast is necessary because DeriveOutputColumns() is not const
  CColRefSet* outputCols =
      const_cast<CExpression*>(expr)->DeriveOutputColumns();
  if (outputCols == nullptr || outputCols->Size() == 0) {
    throw std::runtime_error("EstimateRowSize: No output columns found");
  }

  // all INTs rn. TODO FIX.
  constexpr double AVG_BYTES_PER_COLUMN = 4.0;
  double totalBytes = outputCols->Size() * AVG_BYTES_PER_COLUMN;

  // Ensure we return at least a minimum row size
  return std::max(totalBytes, 8.0);
}

std::string FormatDate(int value) {
    // Extract date components from YYYYMMDD format
    int year = value / 10000;
    int month = (value / 100) % 100;
    int day = value % 100;
    
    // Format as YYYY-MM-DD
    std::stringstream dateStr;
    dateStr << year << "-" 
            << std::setw(2) << std::setfill('0') << month << "-"
            << std::setw(2) << std::setfill('0') << day;
  return dateStr.str();
}


std::unordered_set<std::string> GetOutputColumns(CExpression* expr) {
        std::unordered_set<std::string> requiredColumns;
        CColRefSet *pcrs = expr->DeriveOutputColumns();
        if (pcrs && pcrs->Size() > 0) {
          CColRefSetIter crsi(*pcrs);
          while (crsi.Advance()) {
            CColRef *pcr = crsi.Pcr();
            const CWStringConst *pstr = pcr->Name().Pstr();
            std::string colName = WStringToString(pstr->GetBuffer());
            requiredColumns.insert(colName);
          }
        }
        return requiredColumns;
}

std::unordered_set<std::string> GetSetIntersection(std::unordered_set<std::string> outputColumns, std::unordered_set<std::string> requiredColumns) {
  std::unordered_set<std::string> intersection;

  const auto& smaller = (outputColumns.size() < requiredColumns.size()) ? outputColumns : requiredColumns;
  const auto& larger = (outputColumns.size() < requiredColumns.size()) ? requiredColumns : outputColumns;
    
  for (const auto& item : smaller) {
    if (larger.find(item) != larger.end()) {
      intersection.insert(item);
    }
  }
    
  return intersection;
}

std::unordered_set<std::string>
GetSetDifference(const std::unordered_set<std::string>& outputColumns,
                 const std::unordered_set<std::string>& requiredColumns) {
    std::unordered_set<std::string> diff;
    for (const auto& item : outputColumns) {
        if (requiredColumns.find(item) == requiredColumns.end()) {
          diff.insert(item);
        }
    }
    return diff;
}

Expression CreateProjectListFromColumns(std::unordered_set<std::string> columns) {
  ExpressionArguments projElements;
  for (const auto& col : columns) {
    projElements.push_back(Symbol{col});
    projElements.push_back(Symbol{col});
  }
  return ComplexExpression{"As"_, {}, std::move(projElements), {}};
}

}  // namespace cexpressiontoboss::utils