#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>

#ifdef _WIN32
extern "C" {
__declspec(dllexport) BOSSExpression* evaluate(BOSSExpression* e);
__declspec(dllexport) void reset();
}
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpReserved) {
  switch(fdwReason) {
  case DLL_PROCESS_ATTACH:
  case DLL_THREAD_ATTACH:
  case DLL_THREAD_DETACH:
    break;
  case DLL_PROCESS_DETACH:
    // Make sure to call reset instead of letting destructors to be called.
    // It leaves the engine unique_ptr in a non-dangling state
    // in case the depending process still want to call reset() during its own destruction
    // (which does happen in a unpredictable order if it is itself a dll:
    // https://devblogs.microsoft.com/oldnewthing/20050523-05/?p=35573)
    reset();
    break;
  }
  return TRUE;
}
#endif // _WIN32

#include <af/algorithm.h>
#include <af/array.h>
#include <af/data.h>
#include <af/index.h>
#include <af/internal.h>
#include <af/statistics.h>
#include <af/util.h>
#include <algorithm>
#include <any>
#include <arrayfire.h>
#include <cassert>
#include <chrono>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <list>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>

#include <iostream>

// #define LOGGING_TRANSFER_TO_GPU

using boss::Symbol;
using std::get;

namespace utilities {
// reference counter class to track references for Span and af::array pointers to memory
// calling a destructor once the reference count reaches 0
class SpanReferenceCounter {
public:
  // call destructor only for the initial caller of add(), who is the owner of the data
  void add(void* data, std::function<void(void)>&& destructor = {}) {
    auto& info = map.try_emplace(data, std::move(destructor)).first->second;
    info.counter++;
  }
  void remove(void* data) {
    auto it = map.find(data);
    if(--it->second.counter == 0) {
      if(it->second.destructor) {
        it->second.destructor();
      }
      map.erase(it);
    }
  }
  ~SpanReferenceCounter() { assert(map.empty()); } // NOLINT

  SpanReferenceCounter() = default;
  SpanReferenceCounter(SpanReferenceCounter const&) = delete;
  SpanReferenceCounter& operator=(SpanReferenceCounter const&) = delete;
  SpanReferenceCounter(SpanReferenceCounter&&) = delete;
  SpanReferenceCounter& operator=(SpanReferenceCounter&&) = delete;

private:
  struct Info {
    std::function<void(void)> destructor;
    unsigned int counter = 0;
    explicit Info(std::function<void(void)>&& f) : destructor(std::move(f)) {}
  };
  std::unordered_map<void*, Info> map;
};

// use by copyDataIn/copyDataOut to simulate data movement between engines
static boss::Expression deepCopy(boss::Expression const& expr) {
  if(!std::holds_alternative<boss::ComplexExpression>(expr)) {
    return expr.clone(boss::expressions::CloneReason::FOR_TESTING);
  }
  auto const& e = get<boss::ComplexExpression>(expr);
  auto const& head = e.getHead();
  auto const& dynamics = e.getDynamicArguments();
  auto const& spans = e.getSpanArguments();
  boss::ExpressionArguments dynamicsCopy;
  std::transform(dynamics.begin(), dynamics.end(), std::back_inserter(dynamicsCopy),
                 [](auto const& arg) { return deepCopy(arg); });
  boss::expressions::ExpressionSpanArguments spansCopy;
  std::transform(spans.begin(), spans.end(), std::back_inserter(spansCopy), [](auto const& span) {
    return std::visit(
        [](auto const& typedSpan) -> boss::expressions::ExpressionSpanArgument {
          // do a deep copy of the span
          return typedSpan.clone(boss::expressions::CloneReason::FOR_TESTING);
        },
        span);
  });
  return boss::ComplexExpression(head, {}, std::move(dynamicsCopy), std::move(spansCopy));
}
} // namespace utilities

static utilities::SpanReferenceCounter& spanReferenceCounter() {
  static utilities::SpanReferenceCounter spanReferenceCounter{};
  return spanReferenceCounter;
}

class Pred;

#ifdef ABLATION_NO_FAST_PATH
namespace boss {
template <> struct boss::Span<Pred>;
template <> struct boss::Span<Pred const>;
} // namespace boss
#endif // ABLATION_NO_FAST_PATH

using AFExpressionSystem = boss::expressions::generic::ExtensibleExpressionSystem<Pred>;
using AtomicExpression = AFExpressionSystem::AtomicExpression;
using ComplexExpression = AFExpressionSystem::ComplexExpression;
template <typename... T>
using ComplexExpressionWithStaticArguments =
    AFExpressionSystem::ComplexExpressionWithStaticArguments<T...>;
using Expression = AFExpressionSystem::Expression;
using ExpressionArguments = AFExpressionSystem::ExpressionArguments;
using ExpressionSpanArguments = AFExpressionSystem::ExpressionSpanArguments;
using ExpressionSpanArgument = AFExpressionSystem::ExpressionSpanArgument;
using ExpressionBuilder = boss::utilities::ExtensibleExpressionBuilder<AFExpressionSystem>;
static ExpressionBuilder operator""_(const char* name, size_t /*unused*/) {
  return ExpressionBuilder(name);
};

static auto& properties() {
  static struct {
    std::unordered_set<std::string> cachedColumnsWhitelist;
    uint64_t MaxGPUMemoryCache = uint64_t(-1); // in bytes
    bool copyDataIn = false;                   // for benchmarking the cost of data movement
    bool copyDataOut = false;
    bool disableGather = false;
  } props;
  return props;
}

static auto& state() {
  static struct {
    std::unordered_set<void*> plannedMovedColumns;
    uint64_t plannedGPUMemoryUsage = 0; // in bytes
  } st;
  return st;
}

// to cache the arrays after moving the initial spans from CPU to GPU
// so further queries do not have to move them again
// + caching the partition sizes to retrieve for combined arrays
auto& cachedGPUArrays() {
  static std::unordered_map<void*, std::pair<af::array, std::vector<size_t>>> cache;
  return cache;
}

// to keep track of the selection that occured during a query
// this is used by joins to know if foreign key indexes can be apply
// (if the foreign table has been filtered, the index is invalid)
auto& filteredAttributes() {
  static std::unordered_set<Symbol> filteredSpans;
  return filteredSpans;
}

// list all the table symbols needed downstream.
// used to avoid projecting/selecting unused indexes
auto& usedTableSymbols(bool symbolAsIndex) {
  static std::unordered_set<Symbol> symbolsForColumns;
  static std::unordered_set<Symbol> symbolsForIndexes;
  return symbolAsIndex ? symbolsForColumns : symbolsForIndexes;
}

// Wrapper for the af::array to customise the data ownership:
// - always call afArray.lock() so non-own data is not freed by ArrayFire's internals!
// - use the Span's reference counter (so shared with spans pointing to same memory)
// - the destructor calls afArray.unlock() to free memory (only if ArrayFire owns the data)
template <template <class> class SpanInputType, template <class> class SpanOutputType,
          typename ExpressionSpanArgumentType>
class AfArrayWrapper {
public:
  size_t size() const { return _size; }
  auto begin() const { return pointer; }
  static Pred& at(size_t /*i*/) {
    throw std::runtime_error("at() is not implemented for AfArrayWrapper. "
                             "It should be converted to another boss::Span first.");
  }
  auto operator[](size_t i) const -> decltype(auto) { return at(i); }
  auto operator[](size_t i) -> decltype(auto) { return at(i); }
  ExpressionSpanArgumentType subspan(size_t /*offset*/, size_t /*size*/) {
    throw std::runtime_error("subspan() is not implemented for AfArrayWrapper. "
                             "It should be converted to another boss::Span first.");
  }
  ExpressionSpanArgumentType subspan(size_t /*offset*/) {
    throw std::runtime_error("subspan() is not implemented for AfArrayWrapper. "
                             "It should be converted to another boss::Span first.");
  }

#ifdef ABLATION_NO_FAST_PATH
  template <typename ComplexExpressionType>
  AfArrayWrapper(ComplexExpressionType const& exprPtr, size_t beginIndex, size_t size,
                 void const* /*unused*/)
      : AfArrayWrapper([&exprPtr, &beginIndex]() -> AfArrayWrapper const& {
          for(auto const& baseSpan : exprPtr->getSpanArguments()) {
            if(beginIndex == 0) {
              return get<SpanOutputType<Pred>>(baseSpan);
            }
            beginIndex -= get<SpanOutputType<Pred>>(baseSpan).size();
          }
          throw std::runtime_error("wrong beginIndex, does not match span layouts");
        }() /*execute right away*/) {}
  auto baseBegin() const { return basePointer; }
#endif

  template <typename T> struct DType;
  template <> struct DType<int32_t> : std::integral_constant<af::dtype, s32> {};
  template <> struct DType<int64_t> : std::integral_constant<af::dtype, s64> {};
  template <> struct DType<double_t> : std::integral_constant<af::dtype, f64> {};
  template <>
  struct DType<std::string> : std::integral_constant<af::dtype, u64> {
  }; // this conversion is intended only for casting (in projections) not for calculations!

  template <typename T>
  using AFType =
      std::conditional_t<std::is_same_v<T, int64_t> && sizeof(int64_t) == sizeof(long long),
                         long long, // required on linux because ArrayFire lib is not compiled
                                    // for int64_t (i.e. long)
                         T>;

  explicit AfArrayWrapper(af::array&& arr)
      : _size(arr.dims(0)), afArray(std::move(arr)), pointer(nullptr), isCPUMemory(false),
        cacheToGPU(false)
#ifdef ABLATION_NO_FAST_PATH
        ,
        basePointer(nullptr)
#endif // ABLATION_NO_FAST_PATH
  {
  }

  template <typename T, typename std::enable_if_t<
                            std::negation_v<std::is_same<std::decay_t<T>, Pred>>, bool> = true>
  explicit AfArrayWrapper(SpanInputType<T>&& span, std::string const& name = {})
      : _size(span.size()), afArray(spanToAfArray(span)), pointer(nullptr)
#ifdef ABLATION_NO_FAST_PATH
        ,
        basePointer(nullptr)
#endif // ABLATION_NO_FAST_PATH
        ,
        isCPUMemory(true),
        cacheToGPU(!name.empty() && properties().cachedColumnsWhitelist.contains(name))
#ifdef LOGGING_TRANSFER_TO_GPU
        ,
        name(name)
#endif
  {
    checkAfErrors(af_lock_array(afArray.get()));
    pointer = reinterpret_cast<AFType<std::decay_t<T>>*>(
        const_cast<std::decay_t<T>*>( // NOLINT(cppcoreguidelines-pro-type-const-cast)
            span.begin()));
#ifdef ABLATION_NO_FAST_PATH
    basePointer = reinterpret_cast<AFType<std::decay_t<T>>*>(
        const_cast<std::decay_t<T>*>( // NOLINT(cppcoreguidelines-pro-type-const-cast)
            span.baseBegin()));
#endif // ABLATION_NO_FAST_PATH
    // checkAfErrors(af_get_device_ptr(&pointer, afArray.get())); // automatically calls lock() too
    spanReferenceCounter().add(pointer,
                               [s = std::make_shared<SpanInputType<T>>(std::move(span))]() {});
  } // NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)

  AfArrayWrapper(AfArrayWrapper&& other, std::string const& /*name*/ = {}) noexcept
      : _size(other._size), partitionSizes(std::move(other.partitionSizes)),
        afArray(std::move(other.afArray)), pointer(other.pointer)
#ifdef ABLATION_NO_FAST_PATH
        ,
        basePointer(other.basePointer)
#endif // ABLATION_NO_FAST_PATH
        ,
        isCPUMemory(other.isCPUMemory), cacheToGPU(other.cacheToGPU)
#ifdef LOGGING_TRANSFER_TO_GPU
        ,
        name(std::move(other.name))
#endif
  {
    if(pointer != nullptr) {
      spanReferenceCounter().add(pointer);
    }
  }

  AfArrayWrapper(AfArrayWrapper const& other) noexcept
      : _size(other._size), partitionSizes(other.partitionSizes), afArray(other.afArray),
        pointer(other.pointer)
#ifdef ABLATION_NO_FAST_PATH
        ,
        basePointer(other.basePointer)
#endif // ABLATION_NO_FAST_PATH
        ,
        isCPUMemory(other.isCPUMemory), cacheToGPU(other.cacheToGPU)
#ifdef LOGGING_TRANSFER_TO_GPU
        ,
        name(other.name)
#endif
  {
    if(pointer != nullptr) {
      spanReferenceCounter().add(pointer);
    }
  }

  AfArrayWrapper& operator=(AfArrayWrapper&& other) noexcept {
    _size = other._size;
    partitionSizes = std::move(other.partitionSizes);
    afArray = std::move(other.afArray);
    if(pointer != nullptr) {
      spanReferenceCounter().remove(pointer);
    }
    pointer = other.pointer;
    if(pointer != nullptr) {
      spanReferenceCounter().add(pointer);
    }
#ifdef ABLATION_NO_FAST_PATH
    basePointer = other.basePointer;
#endif // ABLATION_NO_FAST_PATH
    isCPUMemory = other.isCPUMemory;
    cacheToGPU = other.cacheToGPU;
#ifdef LOGGING_TRANSFER_TO_GPU
    name = std::move(other.name);
#endif
    return *this;
  }

  AfArrayWrapper& operator=(AfArrayWrapper const& other) noexcept {
    if(this == &other) {
      return *this;
    }
    _size = other._size;
    partitionSizes = other.partitionSizes;
    afArray = other.afArray;
    if(pointer != nullptr) {
      spanReferenceCounter().remove(pointer);
    }
    pointer = other.pointer;
#ifdef ABLATION_NO_FAST_PATH
    basePointer = other.basePointer;
#endif // ABLATION_NO_FAST_PATH
    if(pointer != nullptr) {
      spanReferenceCounter().add(pointer);
    }
    isCPUMemory = other.isCPUMemory;
    cacheToGPU = other.cacheToGPU;
#ifdef LOGGING_TRANSFER_TO_GPU
    name = other.name;
#endif
    return *this;
  }

  ~AfArrayWrapper() {
    if(pointer != nullptr) {
      spanReferenceCounter().remove(pointer);
    }
  }

  explicit operator boss::expressions::ExpressionSpanArgument() {
    if(afArray.type() == DType<int32_t>::value) {
      return static_cast<SpanOutputType<int32_t>>(*this);
    }
    if(afArray.type() == DType<int64_t>::value) {
      return static_cast<SpanOutputType<int64_t>>(*this);
    }
    if(afArray.type() == DType<double_t>::value) {
      return static_cast<SpanOutputType<double_t>>(*this);
    }
    if(afArray.type() == DType<std::string>::value) {
      return static_cast<SpanOutputType<std::string>>(*this);
    }
    throw std::runtime_error("AFArray: af::array type not supported");
  }

  template <typename T,
            typename std::enable_if_t<!std::is_same_v<std::decay_t<T>, Pred>, bool> = true>
  explicit operator SpanOutputType<T>() {
    if(isCPUMemory) {
      spanReferenceCounter().add(pointer);
      return SpanOutputType<T>(reinterpret_cast<T*>(pointer), afArray.dims(0),
                               [ptr = this->pointer]() { spanReferenceCounter().remove(ptr); });
    }
    // otherwise copy from GPU to CPU
#ifdef LOGGING_TRANSFER_TO_GPU
    auto numBytes = afArray.bytes();
    std::cout << "GPU -> CPU transfer " << numBytes << " for " << name << std::endl;
#endif
    void* hostPtr = nullptr;
    checkAfErrors(af_alloc_host(&hostPtr, afArray.bytes()));
    afArray.host(hostPtr);
    spanReferenceCounter().add(hostPtr, [hostPtr]() { checkAfErrors(af_free_host(hostPtr)); });
    return SpanOutputType<T>(reinterpret_cast<T*>(hostPtr), afArray.dims(0),
                             [hostPtr]() { spanReferenceCounter().remove(hostPtr); });
  };

  explicit operator af::array const &() {
    moveToGPU();
    return afArray;
  }

  uint64_t bytes() const {
    auto elementSize = afArray.type() == DType<int32_t>::value ? sizeof(int32_t) : sizeof(int64_t);
    return elementSize * static_cast<uint64_t>(_size);
  }

  uint64_t bytesToMoveToGPU() const {
    if(!isCPUMemory) {
      return 0;
    }
    return bytes();
  }

  // this function modify the input arrays to make them contiguous in memory:
  // they are forced to be moved to GPU (without caching)
  // and then re-intialised as subarrays of the combined array after executing af::join
  template <typename PartitionType>
  static PartitionType combineOnGPU(std::vector<PartitionType>&& partitions) {
    if(partitions.size() == 1) {
      return std::move(partitions.back());
    }
#ifdef ABLATION_NO_FAST_PATH
    auto referencePointer = partitions[0].basePointer;
#else
    auto referencePointer = partitions[0].pointer;
#endif // ABLATION_NO_FAST_PATH
    auto temp = af::array();
    auto temp2 = std::vector<size_t>();
    auto [combinedArray, partitionSizes, initialised] =
        [&partitions, &temp, &temp2,
         &referencePointer]() mutable -> std::tuple<af::array&, std::vector<size_t>&, bool> {
      if(!partitions[0].cacheToGPU) {
        return {temp, temp2, false};
      }
      auto [it, inserted] = cachedGPUArrays().try_emplace(referencePointer);
      return {it->second.first, it->second.second, !inserted};
    }();
    if(!initialised) {
      size_t totalSize = 0;
      partitionSizes.reserve(partitions.size());
      std::transform(partitions.begin(), partitions.end(), std::back_inserter(partitionSizes),
                     [&totalSize](auto const& partition) {
                       totalSize += partition.size();
                       return partition.size();
                     });
#ifdef LOGGING_TRANSFER_TO_GPU
      auto name = partitions.back().name;
#endif
      // copy directly data to a fully-allocated combined array to avoid fragmentation
      combinedArray = af::array(static_cast<dim_t>(totalSize), partitions.back().afArray.type());
      if(state().plannedMovedColumns.contains(referencePointer)) {
        if(!partitions[0].cacheToGPU) {
          state().plannedMovedColumns.erase(referencePointer);
        }
        if(state().plannedGPUMemoryUsage < totalSize) {
          throw std::runtime_error("tranferred " + std::to_string(totalSize) +
                                   " bytes exceed planned GPU memory usage: " +
                                   std::to_string(state().plannedGPUMemoryUsage));
        }
        state().plannedGPUMemoryUsage -= totalSize;
      }
      size_t startIndex = 0;
      for(auto&& partition : partitions) {
#ifdef LOGGING_TRANSFER_TO_GPU
        std::cout << "merging " << partition.name
                  << (partition.isCPUMemory ? " (from CPU)" : " (from GPU)") << std::endl;
        auto allocs = getAfCurrentAllocationSize();
        std::cout << "used GPU memory: " << allocs << std::endl;
#endif
        auto endIndex = startIndex + partition.size();
        partition.moveToGPU(false);
        combinedArray(af::seq(static_cast<double>(startIndex), static_cast<double>(endIndex - 1))) =
            partition.afArray;
        startIndex = endIndex;
        partition.afArray = af::array(); // release the memory
        checkAfErrors(af_device_gc());
      }
      partitions.clear();
#ifdef LOGGING_TRANSFER_TO_GPU
      std::cout << "merged " << name << std::endl;
      auto allocs = getAfCurrentAllocationSize();
      std::cout << "used GPU memory: " << allocs << std::endl;
#endif
    }
    auto combinedSpan = PartitionType{af::array(combinedArray)};
    combinedSpan.partitionSizes = partitionSizes;
    return std::move(combinedSpan);
  }

  template <typename PartitionType>
  std::vector<PartitionType> resplitIntoPartitionsToMatch(PartitionType const& refSpan) {
    std::vector<PartitionType> partitions;
    size_t startIndex = 0;
    for(size_t size : refSpan.partitionSizes) {
      auto endIndex = startIndex + size;
      partitions.emplace_back(
          afArray(af::seq(static_cast<double>(startIndex), static_cast<double>(endIndex - 1))));
      startIndex = endIndex;
    }
    if(startIndex < _size) {
      partitions.emplace_back(
          afArray(af::seq(static_cast<double>(startIndex), static_cast<double>(_size - 1))));
    }
    return partitions;
  }

  static size_t currentGPUMemoryUsage() {
    checkAfErrors(af_device_gc());
    return getAfCurrentAllocationSize();
  }

private:
  size_t _size;
  std::vector<size_t> partitionSizes; // to retrieve partitions when it has been combined
  af::array afArray;
  void* pointer;
#ifdef ABLATION_NO_FAST_PATH
  void* basePointer;
#endif // ABLATION_NO_FAST_PATH
  bool isCPUMemory;
  bool cacheToGPU;
#ifdef LOGGING_TRANSFER_TO_GPU
  std::string name;
#endif

  void moveToGPU(bool enableCaching = true) {
    if(isCPUMemory) {
#ifdef ABLATION_NO_FAST_PATH
      void* referencePointer = basePointer;
#else
      void* referencePointer = pointer;
#endif // ABLATION_NO_FAST_PATH
      // copy from CPU TO GPU
      auto [GPUArray, partitionSizes] = [&]() -> std::tuple<af::array, std::vector<size_t>> {
        // check if the GPU array has already been cached
        if(cacheToGPU && enableCaching) {
          auto it = cachedGPUArrays().find(referencePointer);
          if(it != cachedGPUArrays().end()) {
#ifdef LOGGING_TRANSFER_TO_GPU
            std::cout << "GPU: get from cache " << name << std::endl;
#endif
            return it->second;
          }
#ifdef LOGGING_TRANSFER_TO_GPU
          else {
            std::cout << "cannot get from cache " << name << " at " << referencePointer
                      << std::endl;
          }
#endif
        }
        af_array afCArray{};
        checkAfErrors(af_create_array(&afCArray, pointer, afArray.dims().ndims(),
                                      afArray.dims().get(), afArray.type()));
        af::array arr = af::array(afCArray);
        if(state().plannedMovedColumns.contains(referencePointer)) {
          if(!cacheToGPU) {
            state().plannedMovedColumns.erase(referencePointer);
          }
          auto transferredBytes = bytes();
          if(state().plannedGPUMemoryUsage < transferredBytes) {
            throw std::runtime_error("tranferred " + std::to_string(transferredBytes) +
                                     " bytes exceed planned GPU memory usage: " +
                                     std::to_string(state().plannedGPUMemoryUsage));
          }
          state().plannedGPUMemoryUsage -= transferredBytes;
        }
#ifdef LOGGING_TRANSFER_TO_GPU
        auto numBytes = arr.bytes();
        std::cout << "CPU -> GPU transfer " << numBytes << " for " << name << std::endl;
        auto allocs = getAfCurrentAllocationSize();
        std::cout << "used GPU memory: " << allocs << std::endl;
#endif
        if(cacheToGPU && enableCaching) {
          // only the initial input Spans will take this path, always cache them.
          cachedGPUArrays()[referencePointer] = {arr, std::vector<size_t>()};
#ifdef LOGGING_TRANSFER_TO_GPU
          std::cout << "GPU: cache " << name << " at " << referencePointer << std::endl;
#endif
        }
        return {arr, std::vector<size_t>()};
      }();
      // re-initialise this AfArrayWrapper as GPU memory
      *this = AfArrayWrapper(std::move(GPUArray));
      this->partitionSizes.swap(partitionSizes);
    }
  }

  template <typename T> static af::array spanToAfArray(SpanInputType<T> const& span) {
    if(span.size() == 0) {
      return af::array(0, DType<std::decay_t<T>>::value);
    }
    af_array afCArray{};
    auto* data = reinterpret_cast<AFType<std::decay_t<T>>*>(
        const_cast<std::decay_t<T>*>( // NOLINT(cppcoreguidelines-pro-type-const-cast)
            span.begin()));
    std::array<dim_t, 1> dims{static_cast<dim_t>(span.size())};
    // Device mode rather than host to avoid copying data in.
    // With device mode, AF takes ownership of the pointer!
    // but AfAray always lock() the array in the constructor to avoid double free
    checkAfErrors(af_device_array(&afCArray, data, 1, dims.data(), DType<std::decay_t<T>>::value));
    af::array afArray(afCArray);
    return afArray;
  }

  static void checkAfErrors(af_err err) {
    if(err != AF_SUCCESS) {
      throw []() {
        char* msg = nullptr; // NOLINT
        af_get_last_error(&msg, nullptr);
        af::exception ex(msg);
        af_free_host(msg);
        return ex;
      }();
    }
  }

  static size_t getAfCurrentAllocationSize() {
    size_t allocs = 0;
    size_t allocs2 = 0;
    size_t locks = 0;
    size_t locks2 = 0;
    checkAfErrors(af_device_mem_info(&allocs, &allocs2, &locks, &locks2));
    return allocs;
  }
};

// In addition, this class is exposed as a specialization of a Span,
// so it can be passed through nested operators without converting yet from/to Span
// (and unnecessary moving data between GPU and CPU too early).
// Only when converting to boss::SpanArgument, then they are evaluated and copied back to
// namespace boss {
// template <> struct Span<Pred> : public AfArrayWrapper<boss::Span, ExpressionSpanArgument> {
// public:
//   using AfArrayWrapper<boss::Span, ExpressionSpanArgument>::AfArrayWrapper;
// };
// template <> struct Span<Pred const> : public AfArrayWrapper<boss::Span, ExpressionSpanArgument> {
// public:
//   using AfArrayWrapper<boss::Span, ExpressionSpanArgument>::AfArrayWrapper;
// };
// } // namespace boss
// template <> class Span<Pred> : public boss::Span<Pred> {
// public:
//   using BaseType = boss::expressions::atoms::SlowPathSpan<Pred>;
//   using boss::Span<Pred>::Span;
// };
// template <> class Span<Pred const> : public boss::Span<Pred const> {
// public:
//   using BaseType = boss::expressions::atoms::SlowPathSpan<Pred const>;
//   using boss::Span<Pred const>::Span;
// };
#ifdef ABLATION_NO_FAST_PATH
template <>
class boss::Span<Pred> : public AfArrayWrapper<boss::expressions::atoms::SlowPathSpan, boss::Span,
                                               boss::expressions::SlowPathSpanArgument> {
public:
  using element_type = Pred;
  using AfArrayWrapper<boss::expressions::atoms::SlowPathSpan, boss::Span,
                       boss::expressions::SlowPathSpanArgument>::AfArrayWrapper;
};
template <>
class boss::Span<Pred const>
    : public AfArrayWrapper<boss::expressions::atoms::SlowPathSpan, boss::Span,
                            boss::expressions::SlowPathSpanArgument> {
public:
  using element_type = Pred const;
  using AfArrayWrapper<boss::expressions::atoms::SlowPathSpan, boss::Span,
                       boss::expressions::SlowPathSpanArgument>::AfArrayWrapper;
};
template <> class boss::expressions::atoms::SlowPathSpan<Pred> : public boss::Span<Pred> {
public:
  using boss::Span<Pred>::Span;
};
template <>
class boss::expressions::atoms::SlowPathSpan<Pred const> : public boss::Span<Pred const> {
public:
  using boss::Span<Pred const>::Span;
};
#else
template <>
class boss::Span<Pred> : public AfArrayWrapper<boss::Span, boss::Span, ::ExpressionSpanArgument> {
public:
  using element_type = Pred;
  using AfArrayWrapper<boss::Span, boss::Span, ::ExpressionSpanArgument>::AfArrayWrapper;
};
template <>
class boss::Span<Pred const>
    : public AfArrayWrapper<boss::Span, boss::Span, ::ExpressionSpanArgument> {
public:
  using element_type = Pred const;
  using AfArrayWrapper<boss::Span, boss::Span, ::ExpressionSpanArgument>::AfArrayWrapper;
};
#endif // ABLATION_NO_FAST_PATH

#ifdef ABLATION_NO_FAST_PATH
template <typename T> using Span = boss::expressions::atoms::SlowPathSpan<T>;
#else
template <typename T> using Span = boss::expressions::atoms::Span<T>;
#endif

class Pred : public std::function<std::optional<Span<Pred>>(ExpressionArguments&, bool, bool)> {
public:
  using Function = std::function<std::optional<Span<Pred>>(ExpressionArguments&, bool, bool)>;
  template <typename F>
  Pred(F&& func, boss::Expression&& expr)
      : Function(std::forward<F>(func)), cachedExpr(std::move(expr)) {}
  template <typename F>
  Pred(F&& func, boss::Symbol const& s) : Function(std::forward<F>(func)), cachedExpr(s) {}
  Pred(Pred&& other) noexcept
      : Function(std::move(static_cast<Function&&>(other))),
        cachedExpr(std::move(other.cachedExpr)) {}
  Pred& operator=(Pred&& other) noexcept {
    *static_cast<Function*>(this) = std::move(static_cast<Function&&>(other));
    cachedExpr = std::move(other.cachedExpr);
    return *this;
  }
  Pred(Pred const&) = delete;
  Pred const& operator=(Pred const&) = delete;
  ~Pred() = default;

  friend ::std::ostream& operator<<(std::ostream& out, Pred const& pred) {
    out << "[Pred for " << pred.cachedExpr << "]";
    return out;
  }

  explicit operator boss::Expression() && { return std::move(cachedExpr); }

private:
  boss::Expression cachedExpr; // so we can revert it back if unused
};

static boss::expressions::ExpressionSpanArgument toBOSSExpression(ExpressionSpanArgument&& span) {
  return std::visit(
      []<typename T>(boss::Span<T>&& typedSpan) -> boss::expressions::ExpressionSpanArgument {
        return static_cast<boss::expressions::ExpressionSpanArgument>(std::move(typedSpan));
      },
      std::move(span));
}

static boss::Expression toBOSSExpression(Expression&& expr, bool isPredicate = false) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& e) -> boss::Expression {
            auto [head, unused_, dynamics, spans] = std::move(e).decompose();
            int NChildIsPredicate = dynamics.size(); // no child is a predicate as default
            if(head == "Select"_) {
              NChildIsPredicate = 1; // Select(relation, predicate)
            } else if(head == "Join"_) {
              NChildIsPredicate = 2; // Join(relation1, relation2, predicate)
            } else if(head == "Table"_) {
              // filter out the indexes
              ExpressionArguments filteredDynamics;
              for(auto&& column : dynamics) {
                if(get<ComplexExpression>(column).getHead() == "Index"_) {
                  continue;
                }
                filteredDynamics.emplace_back(std::move(column));
              }
              filteredDynamics.swap(dynamics);
            }
            boss::ExpressionArguments bossDynamics;
            bossDynamics.reserve(dynamics.size());
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           std::back_inserter(bossDynamics), [&](auto&& arg) {
                             return toBOSSExpression(std::forward<decltype(arg)>(arg),
                                                     NChildIsPredicate-- == 0);
                           });
            boss::expressions::ExpressionSpanArguments bossSpans;
            bossSpans.reserve(spans.size());
#ifdef ABLATION_NO_FAST_PATH
            auto afSpans = (ExpressionSpanArguments &&) std::move(spans);
            std::transform(
                std::make_move_iterator(afSpans.begin()), std::make_move_iterator(afSpans.end()),
                std::back_inserter(bossSpans),
                [](auto&& span) { return toBOSSExpression(std::forward<decltype(span)>(span)); });
#else
            std::transform(
                std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
                std::back_inserter(bossSpans),
                [](auto&& span) { return toBOSSExpression(std::forward<decltype(span)>(span)); });
#endif // ABLATION_NO_FAST_PATH
            auto output = boss::ComplexExpression(std::move(head), {}, std::move(bossDynamics),
                                                  std::move(bossSpans));
            if(isPredicate && output.getHead() != "Where"_) {
              // make sure to re-inject "Where" clause before the predicate
              boss::ExpressionArguments whereArgs;
              whereArgs.emplace_back(std::move(output));
              return boss::ComplexExpression("Where"_, std::move(whereArgs));
            }
            return std::move(output);
          },
          [&](Pred&& e) -> boss::Expression {
            // remaining unevaluated internal predicate, switch back to the initial expression
            auto output = static_cast<boss::Expression>(std::move(e));
            if(isPredicate && (!std::holds_alternative<boss::ComplexExpression>(output) ||
                               std::get<boss::ComplexExpression>(output).getHead() != "Where"_)) {
              // make sure to re-inject "Where" clause before the predicate
              boss::ExpressionArguments whereArgs;
              whereArgs.emplace_back(std::move(output));
              return boss::ComplexExpression("Where"_, std::move(whereArgs));
            }
            return std::move(output);
          },
          [](auto&& otherTypes) -> boss::Expression { return otherTypes; }),
      std::move(expr));
}

static Expression evaluateInternal(Expression&& e);

template <typename... StaticArgumentTypes>
ComplexExpressionWithStaticArguments<StaticArgumentTypes...>
transformDynamicsToSpans(ComplexExpressionWithStaticArguments<StaticArgumentTypes...>&& input_) {
  using SpanInputs =
      std::variant</*std::vector<bool>, */ std::vector<std::int32_t>, std::vector<std::int64_t>,
                   std::vector<std::double_t>, std::vector<std::string> /*, std::vector<Symbol>*/>;
  std::vector<SpanInputs> spanInputs;
#ifdef ABLATION_NO_FAST_PATH
  auto [head, statics, dynamics, oldSpans] = std::move(input_).fastPathDecompose();
#else
  auto [head, statics, dynamics, oldSpans] = std::move(input_).decompose();
#endif // ABLATION_NO_FAST_PATH

  auto it = std::move_iterator(dynamics.begin());
  for(; it != std::move_iterator(dynamics.end()); ++it) {
    if(!std::visit(
           [&spanInputs]<typename InputType>(InputType&& argument) {
             using Type = std::decay_t<InputType>;
             if constexpr(boss::utilities::isVariantMember<std::vector<Type>, SpanInputs>::value) {
               if(!spanInputs.empty() &&
                  std::holds_alternative<std::vector<Type>>(spanInputs.back())) {
                 std::get<std::vector<Type>>(spanInputs.back()).push_back(argument);
               } else {
                 spanInputs.push_back(std::vector<Type>{argument});
               }
               return true;
             }
             return false;
           },
           evaluateInternal(*it))) {
      break;
    }
  }
  dynamics.erase(dynamics.begin(), it.base());

  ExpressionSpanArguments spans;
  spans.reserve(spanInputs.size() + oldSpans.size());
  std::transform(
      std::move_iterator(spanInputs.begin()), std::move_iterator(spanInputs.end()),
      std::back_inserter(spans), [](auto&& untypedInput) {
        return std::visit(
            []<typename Element>(std::vector<Element>&& input) -> ExpressionSpanArgument {
              auto* ptr = input.data();
              auto size = input.size();
              spanReferenceCounter().add(ptr, [v = std::move(input)]() {});
              return boss::Span<Element>(ptr, size,
                                         [ptr]() { spanReferenceCounter().remove(ptr); });
            },
            std::forward<decltype(untypedInput)>(untypedInput));
      });

  std::copy(std::move_iterator(oldSpans.begin()), std::move_iterator(oldSpans.end()),
            std::back_inserter(spans));
  return {std::move(head), std::move(statics), std::move(dynamics), std::move(spans)};
}

Expression transformDynamicsToSpans(Expression&& input) {
  return std::visit(
      [](auto&& x) -> Expression {
        if constexpr(std::is_same_v<std::decay_t<decltype(x)>, ComplexExpression>) {
          return transformDynamicsToSpans(std::forward<decltype(x)>(x));
        } else {
          return std::forward<decltype(x)>(x);
        }
      },
      std::move(input));
}

template <typename... Args> class TypedFunctor;
class Functor {
public:
  virtual ~Functor() = default;
  Functor() = default;
  Functor(Functor const&) = delete;
  Functor const& operator=(Functor const&) = delete;
  Functor(Functor&&) = delete;
  Functor const& operator=(Functor&&) = delete;
  virtual std::pair<Expression, bool> operator()(ComplexExpression&& e) = 0;
  template <typename Func> static std::unique_ptr<Functor> makeUnique(Func&& func) {
    return std::unique_ptr<Functor>(new TypedFunctor(std::forward<decltype(func)>(func)));
  }
};

template <typename... Args> class TypedFunctor : public Functor {
public:
  ~TypedFunctor() override = default;
  TypedFunctor(TypedFunctor const&) = delete;
  TypedFunctor const& operator=(TypedFunctor const&) = delete;
  TypedFunctor(TypedFunctor&&) = delete;
  TypedFunctor const& operator=(TypedFunctor&&) = delete;
  explicit TypedFunctor(
      std::function<Expression(ComplexExpressionWithStaticArguments<Args...>&&)> f)
      : func(f) {}
  std::pair<Expression, bool> operator()(ComplexExpression&& e) override {
    return dispatchAndEvaluate(std::move(e));
  }

private:
  std::function<Expression(ComplexExpressionWithStaticArguments<Args...>&&)> func;
  template <typename... T>
  std::pair<Expression, bool> dispatchAndEvaluate(ComplexExpressionWithStaticArguments<T...>&& e) {
    auto [head, statics, dynamics, spans] = std::move(e).decompose();
    if constexpr(sizeof...(T) < sizeof...(Args)) {
      Expression dispatchArgument =
          dynamics.empty()
              ? std::visit(
                    [](auto& a) -> Expression {
                      if constexpr(std::is_same_v<std::decay_t<decltype(a)>, Span<Pred const>>) {
                        throw std::runtime_error(
                            "Found a Span<Pred const> in an expression to evaluate. "
                            "It should not happen.");
                      } else if constexpr(std::is_same_v<std::decay_t<decltype(a)>, Span<bool>>) {
                        return bool(a[0]);
                      } else {
                        return std::move(a[0]);
                      }
                    },
                    spans.front())
              : std::move(dynamics.at(sizeof...(T)));
      if(dynamics.empty()) {
#ifdef ABLATION_NO_FAST_PATH
        throw std::runtime_error("subspan(1) not supported for the ablation study version.");
#else
        spans[0] = std::visit(
            [](auto&& span) -> ExpressionSpanArgument {
              return std::forward<decltype(span)>(span).subspan(1);
            },
            std::move(spans[0]));
#endif // ABLATION_NO_FAST_PATH
      }
      return std::visit(
          [head = std::move(head), statics = std::move(statics), dynamics = std::move(dynamics),
           spans = std::move(spans), this](auto&& argument) mutable -> std::pair<Expression, bool> {
            typedef std::decay_t<decltype(argument)> ArgType;

            if constexpr(std::is_same_v<ArgType,
                                        std::tuple_element_t<sizeof...(T), std::tuple<Args...>>>) {
              // argument type matching, add one more static argument to the expression
              return dispatchAndEvaluate(ComplexExpressionWithStaticArguments<T..., ArgType>(
                  head,
                  std::tuple_cat(std::move(statics),
                                 std::make_tuple(std::forward<decltype(argument)>(argument))),
                  std::move(dynamics), std::move(spans)));
            } else {
              ExpressionArguments rest{};
              rest.emplace_back(std::forward<decltype(argument)>(argument));
              if(dynamics.size() > sizeof...(Args)) {
                std::copy(std::move_iterator(next(dynamics.begin(), sizeof...(Args))),
                          std::move_iterator(dynamics.end()), std::back_inserter(rest));
              }
              // failed to match the arguments, return the non/semi-evaluated expression
              return std::make_pair(
                  ComplexExpressionWithStaticArguments<T...>(std::move(head), std::move(statics),
                                                             std::move(rest), std::move(spans)),
                  false);
            }
          },
          evaluateInternal(std::move(dispatchArgument)));
    } else {
      ExpressionArguments rest{};
      if(dynamics.size() > sizeof...(Args)) {
        std::transform(std::move_iterator(next(dynamics.begin(), sizeof...(Args))),
                       std::move_iterator(dynamics.end()), std::back_inserter(rest),
                       [](auto&& arg) {
                         // evaluate the rest of the arguments
                         return evaluateInternal(std::forward<decltype(arg)>(arg));
                       });
      }
      // all the arguments are matching, call the function and return the evaluated expression
      return std::make_pair(
          func(ComplexExpressionWithStaticArguments<Args...>(std::move(head), std::move(statics),
                                                             std::move(rest), std::move(spans))),
          true);
    }
  }
};

// from https://stackoverflow.com/a/7943765
template <typename F> struct function_traits : public function_traits<decltype(&F::operator())> {};
template <typename ClassType, typename ReturnType, typename... Args>
struct function_traits<ReturnType (ClassType::*)(Args...) const> {
  typedef ReturnType result_type;
  typedef std::tuple<Args...> args;
};

template <typename F>
concept supportsFunctionTraits = requires(F&& f) {
  typename function_traits<decltype(&F::operator())>;
};

class StatelessOperator {
public:
  std::map<std::type_index, std::unique_ptr<Functor>> functors;

  Expression operator()(ComplexExpression&& e) {
    for(auto&& [id, f] : functors) {
      auto [output, success] = (*f)(std::move(e));
      if(success) {
        return std::move(output);
      }
      e = std::get<ComplexExpression>(std::move(output));
    }
    return std::move(e);
  }

  template <typename F, typename... Types>
  void registerFunctorForTypes(F f, std::variant<Types...> /*unused*/) {
    (
        [this, &f]() {
          if constexpr(std::is_invocable_v<F, ComplexExpressionWithStaticArguments<Types>>) {
            this->functors[typeid(Types)] = Functor::makeUnique(
                std::function<Expression(ComplexExpressionWithStaticArguments<Types> &&)>(f));
          }
        }(),
        ...);
  }

  template <typename F> StatelessOperator& operator=(F&& f) requires(!supportsFunctionTraits<F>) {
    registerFunctorForTypes(f, Expression{});
    if constexpr(std::is_invocable_v<F, ComplexExpression>) {
      this->functors[typeid(ComplexExpression)] =
          Functor::makeUnique(std::function<Expression(ComplexExpression &&)>(f));
    }
    return *this;
  }

  template <typename F> StatelessOperator& operator=(F&& f) requires supportsFunctionTraits<F> {
    using FuncInfo = function_traits<F>;
    using ComplexExpressionArgs = std::tuple_element_t<0, typename FuncInfo::args>;
    this->functors[typeid(ComplexExpressionArgs /*::StaticArgumentTypes*/)] = Functor::makeUnique(
        std::function<Expression(ComplexExpressionArgs &&)>(std::forward<F>(f)));
    return *this;
  }
};

template <typename T>
concept NumericType = requires(T param) {
  requires std::is_integral_v<T> || std::is_floating_point_v<T>;
  requires !std::is_same_v<bool, T>;
  requires std::is_arithmetic_v<decltype(param + 1)>;
  requires !std::is_pointer_v<T>;
};

class OperatorMap : public std::unordered_map<boss::Symbol, StatelessOperator> {
public:
  OperatorMap() {
    (*this)["Set"_] = [](ComplexExpressionWithStaticArguments<Symbol, bool>&& input) -> Expression {
      auto const& key = get<0>(input.getStaticArguments());
      auto value = get<1>(input.getStaticArguments());
      if(key == "ArrayFireEngineCopyDataIn"_) {
        properties().copyDataIn = static_cast<bool>(value);
        return true;
      }
      if(key == "ArrayFireEngineCopyDataOut"_) {
        properties().copyDataOut = static_cast<bool>(value);
        return true;
      }
      if(key == "DisableGatherOperator"_) {
        properties().disableGather = static_cast<bool>(value);
        return true;
      }
      return std::move(input);
    };
    (*this)["Set"_] =
        [](ComplexExpressionWithStaticArguments<Symbol, int64_t>&& input) -> Expression {
      auto const& key = get<0>(input.getStaticArguments());
      auto value = get<1>(input.getStaticArguments());
      if(key == "MaxGPUMemoryCacheMB"_) {
        static auto const MBtoBytes = 20U;
        properties().MaxGPUMemoryCache = static_cast<uint64_t>(value) << MBtoBytes;
        return true;
      }
      if(key == "MaxGPUMemoryCache"_) {
        properties().MaxGPUMemoryCache = static_cast<uint64_t>(value);
        return true;
      }
      return std::move(input);
    };
    (*this)["Set"_] =
        [](ComplexExpressionWithStaticArguments<Symbol, Symbol>&& input) -> Expression {
      auto const& key = get<0>(input.getStaticArguments());
      auto const& value = get<1>(input.getStaticArguments());
      if(key == "CachedColumn"_) {
        properties().cachedColumnsWhitelist.insert(value.getName());
        return true;
      }
      return false;
    };
    (*this)["Plus"_] =
        []<NumericType FirstArgument>(
            ComplexExpressionWithStaticArguments<FirstArgument>&& input) -> Expression {
      input = transformDynamicsToSpans(std::move(input));
      auto [head, statics, dynamics, spans] = std::move(input).decompose();
      auto initialValue = get<0>(statics);
      return std::accumulate(
          std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
          initialValue, [](auto accumulator, auto&& span) {
            return accumulator +
                   af::sum<FirstArgument>(static_cast<af::array>(
                       Span<Pred>(get<Span<FirstArgument>>(std::forward<decltype(span)>(span)))));
          });
    };
    (*this)["Plus"_] = [](ComplexExpressionWithStaticArguments<Symbol>&& input) -> Expression {
      return createLambdaExpression<2>(std::move(input), std::plus());
    };
    (*this)["Plus"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      return createLambdaExpression<2>(std::move(input), std::plus());
    };

    (*this)["Times"_] =
        []<NumericType FirstArgument>(
            ComplexExpressionWithStaticArguments<FirstArgument>&& input) -> Expression {
      input = transformDynamicsToSpans(std::move(input));
      auto [head, statics, dynamics, spans] = std::move(input).decompose();
      auto initialValue = get<0>(statics);
      return std::accumulate(
          std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
          initialValue, [](auto accumulator, auto&& span) {
            return accumulator *
                   af::product<FirstArgument>(static_cast<af::array>(
                       Span<Pred>(get<Span<FirstArgument>>(std::forward<decltype(span)>(span)))));
          });
    };
    (*this)["Times"_] = [](ComplexExpressionWithStaticArguments<Symbol>&& input) -> Expression {
      return createLambdaExpression<2>(std::move(input), std::multiplies());
    };
    (*this)["Times"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      return createLambdaExpression<2>(std::move(input), std::multiplies());
    };

    (*this)["Minus"_] =
        []<NumericType FirstArgument>(
            ComplexExpressionWithStaticArguments<FirstArgument>&& input) -> Expression {
      assert(input.getSpanArguments().empty());        // NOLINT
      assert(input.getDynamicArguments().size() == 1); // NOLINT
      if(std::holds_alternative<Symbol>(input.getDynamicArguments().at(0)) ||
         std::holds_alternative<Pred>(input.getDynamicArguments().at(0))) {
        return createLambdaExpression<2, 2>(std::move(input), std::minus());
      }
      return get<0>(input.getStaticArguments()) -
             get<FirstArgument>(input.getDynamicArguments().at(0));
    };
    (*this)["Minus"_] = [](ComplexExpressionWithStaticArguments<Symbol>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::minus());
    };
    (*this)["Minus"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::minus());
    };

    (*this)["Divide"_] =
        []<NumericType FirstArgument>(
            ComplexExpressionWithStaticArguments<FirstArgument>&& input) -> Expression {
      assert(input.getSpanArguments().empty());        // NOLINT
      assert(input.getDynamicArguments().size() == 1); // NOLINT
      if(std::holds_alternative<Symbol>(input.getDynamicArguments().at(0)) ||
         std::holds_alternative<Pred>(input.getDynamicArguments().at(0))) {
        return createLambdaExpression<2, 2>(std::move(input), std::divides());
      }
      return get<0>(input.getStaticArguments()) /
             get<FirstArgument>(input.getDynamicArguments().at(0));
    };
    (*this)["Divide"_] = [](ComplexExpressionWithStaticArguments<Symbol>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::divides());
    };
    (*this)["Divide"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::divides());
    };

    (*this)["Greater"_] =
        []<NumericType FirstArgument>(
            ComplexExpressionWithStaticArguments<FirstArgument>&& input) -> Expression {
      assert(input.getSpanArguments().empty());        // NOLINT
      assert(input.getDynamicArguments().size() == 1); // NOLINT
      if(std::holds_alternative<Symbol>(input.getDynamicArguments().at(0)) ||
         std::holds_alternative<Pred>(input.getDynamicArguments().at(0))) {
        return createLambdaExpression<2, 2>(std::move(input), std::greater());
      }
      return get<0>(input.getStaticArguments()) >
             get<FirstArgument>(input.getDynamicArguments().at(0));
    };
    (*this)["Greater"_] = [](ComplexExpressionWithStaticArguments<Symbol>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::greater());
    };
    (*this)["Greater"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::greater());
    };

    (*this)["And"_] = [](ComplexExpressionWithStaticArguments<Pred, Pred>&& input) -> Expression {
      return createLambdaExpression<2>(std::move(input), std::logical_and());
    };
    (*this)["Or"_] = [](ComplexExpressionWithStaticArguments<Pred, Pred>&& input) -> Expression {
      return createLambdaExpression<2>(std::move(input), std::logical_or());
    };
    (*this)["Not"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      return createLambdaExpression<1, 1>(std::move(input), std::logical_not());
    };

    (*this)["Year"_] = [](ComplexExpressionWithStaticArguments<Symbol>&& input) -> Expression {
      return createLambdaExpression<1, 1>(std::move(input), [](auto&& epochInDays) {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
        auto offset = 1970.0 * 365.0 + 1969.0 * 0.25 - 1899.0 * 0.01 + (1900.0 - 299.0) * 0.025;
        auto multiplier = 1.0 / (365.0 + 0.25 - 0.01 + 0.025); // NOLINT
        return ((epochInDays + offset) * multiplier).as(s64);
      });
    };

    (*this)["Equal"_] =
        []<NumericType FirstArgument>(
            ComplexExpressionWithStaticArguments<FirstArgument>&& input) -> Expression {
      assert(input.getSpanArguments().empty());        // NOLINT
      assert(input.getDynamicArguments().size() == 1); // NOLINT
      if(std::holds_alternative<Symbol>(input.getDynamicArguments().at(0)) ||
         std::holds_alternative<Pred>(input.getDynamicArguments().at(0))) {
        return createLambdaExpression<2, 2>(std::move(input), std::equal_to());
      }
      return get<0>(input.getStaticArguments()) ==
             get<FirstArgument>(input.getDynamicArguments().at(0));
    };
    //(*this)["Equal"_] = [](ComplexExpressionWithStaticArguments<Symbol, Symbol>&& input) ->
    // Expression {
    //  return createLambdaExpression<2, 2>(std::move(input), []() {
    //          // TODO: implement special case for equi-join predicate?
    //          // however, with current design, we have no access to the indexes
    //      });
    //};
    (*this)["Equal"_] = [](ComplexExpressionWithStaticArguments<Symbol>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::equal_to());
    };
    (*this)["Equal"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      return createLambdaExpression<2, 2>(std::move(input), std::equal_to());
    };

    (*this)["Where"_] = [](ComplexExpressionWithStaticArguments<Pred>&& input) -> Expression {
      assert(input.getSpanArguments().empty());    // NOLINT
      assert(input.getDynamicArguments().empty()); // NOLINT
      return get<0>(std::move(input).getStaticArguments());
    };

    (*this)["As"_] = [](ComplexExpression&& input) -> Expression {
      assert(input.getSpanArguments().empty());            // NOLINT
      assert(input.getDynamicArguments().size() % 2 == 0); // NOLINT
      auto dynamicArguments = std::move(input).getDynamicArguments();
      ExpressionArguments newArguments;
      for(auto argIt = std::make_move_iterator(dynamicArguments.begin());
          argIt != std::make_move_iterator(dynamicArguments.end()); ++argIt) {
        // odd args must be Symbol
        auto name = get<Symbol>(std::move(*argIt++));
        auto projection = std::move(*argIt);
        if(std::holds_alternative<Symbol>(projection)) {
          // even args are converted from Symbol to Pred
          auto const& projSymbol = get<Symbol>(projection);
          if(filteredAttributes().erase(projSymbol) > 0) {
            filteredAttributes().insert(name);
          }
          newArguments.emplace_back(std::move(name));
          newArguments.emplace_back(Pred{createLambdaArgument(projSymbol), std::move(projection)});
        } else {
          newArguments.emplace_back(std::move(name));
          newArguments.emplace_back(std::move(projection));
        }
      }
      std::transform(std::make_move_iterator(dynamicArguments.begin()),
                     std::make_move_iterator(dynamicArguments.end()), dynamicArguments.begin(),
                     [index = 0](auto&& arg) mutable -> Expression {
                       if(index++ % 2 == 0) {
                         // odd args must be Symbol
                         assert(std::holds_alternative<Symbol>(arg)); // NOLINT
                       } else if(std::holds_alternative<Symbol>(arg)) {
                         // even args are converted from Symbol to Pred
                         return Pred{createLambdaArgument(get<Symbol>(arg)), get<Symbol>(arg)};
                       }
                       return std::forward<decltype(arg)>(arg);
                     });
      return ComplexExpression("As"_, std::move(newArguments));
    };

    (*this)["Symbol"_] =
        [](ComplexExpressionWithStaticArguments<std::string>&& input) -> Expression {
      assert(input.getSpanArguments().empty());    // NOLINT
      assert(input.getDynamicArguments().empty()); // NOLINT
      return boss::Symbol(get<0>(input.getStaticArguments()));
    };

    (*this)["StringJoin"_] =
        [](ComplexExpressionWithStaticArguments<std::string>&& input) -> Expression {
      auto [head, statics, dynamics, spans] = std::move(input).decompose();
      auto x = std::accumulate(
          spans.begin(), spans.end(), get<0>(statics), [](auto /*accumulator*/, auto& span) {
            return std::accumulate(get<Span<std::string>>(span).begin(),
                                   get<Span<std::string>>(span).end(), std::string());
          });
      return std::accumulate(
          std::make_move_iterator(dynamics.begin()), std::make_move_iterator(dynamics.end()), x,
          [](auto accumulator, Expression&& dynamicArgument) {
            return accumulator + get<std::string>(evaluateInternal(std::move(dynamicArgument)));
          });
    };

    (*this)["DateObject"_] =
        [](ComplexExpressionWithStaticArguments<std::string>&& input) -> Expression {
      assert(input.getSpanArguments().empty());    // NOLINT
      assert(input.getDynamicArguments().empty()); // NOLINT
      auto str = get<0>(std::move(input).getStaticArguments());
      std::istringstream iss;
      iss.str(std::string(str));
      struct std::tm tm = {};
      iss >> std::get_time(&tm, "%Y-%m-%d");
      auto t = std::mktime(&tm);
      static int const hoursInADay = 24;
      return (int32_t)(std::chrono::duration_cast<std::chrono::hours>(
                           std::chrono::system_clock::from_time_t(t).time_since_epoch())
                           .count() /
                       hoursInADay);
    };

    (*this)["Project"_] =
        [](ComplexExpression /*WithStaticArguments<ComplexExpression, ComplexExpression>*/&&
               inputExpr) -> Expression {
      ExpressionArguments args = std::move(inputExpr).getArguments();
      auto it = std::make_move_iterator(args.begin());
      auto relation = boss::get<ComplexExpression>(std::move(*it));
      auto asExpr = std::move(*++it);
      if(relation.getHead().getName() != "Table") {
        // return unevaluated
        return "Project"_(std::move(relation), std::move(asExpr));
      }
      // auto [relation, asExpr] = std::move(inputExpr).getStaticArguments();
      auto columns = std::move(relation).getDynamicArguments();
      std::transform(
          std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
          columns.begin(), [](auto&& columnExpr) {
            auto column = get<ComplexExpression>(std::forward<decltype(columnExpr)>(columnExpr));
            auto [head, unused_, dynamics, spans] = std::move(column).decompose();
            auto list = get<ComplexExpression>(std::move(dynamics.at(1)));
            if(list.getHead() == "DictionaryEncodedList"_) {
              // TODO: proper handling of this type of string column
              // Ideally, returning complex expressions rather than spans during projection,
              // so we can pass over the whole wrapper (including the string buffer)
              auto [unused1, unused2, listDynamics, unused3] = std::move(list).decompose();
              list = std::move(get<ComplexExpression>(listDynamics[0]));
            }
            dynamics.at(1) = transformDynamicsToSpans(std::move(list));
            return ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
          });
      auto projectedColumns = ExpressionArguments{};
      ExpressionArguments asArgs = boss::get<ComplexExpression>(std::move(asExpr)).getArguments();
      auto asItEnd = std::make_move_iterator(asArgs.end());
      for(auto asIt = std::make_move_iterator(asArgs.begin()); asIt != asItEnd; ++asIt) {
        auto name = get<Symbol>(std::move(*asIt));
        auto asFunc = get<Pred>(std::move(*++asIt));
        ExpressionSpanArguments spans{};
        auto constexpr merge = false;         // keep the partitions
        auto constexpr transferToGPU = false; // no need to transfer unless calculation is needed
        while(auto projected = asFunc(columns, merge, transferToGPU)) {
          spans.emplace_back(*projected);
        }
        if(spans.empty()) {
          // There was no span to process.
          // Most likely, we ran out of GPU memory, and the filtering is unfinished.
          // Return the expression as (semi-)unevaluated:
          // we combine the already processed projections
          // and wrap the remaining ones in a new Project
          auto newAsExprs = ExpressionArguments{};
          newAsExprs.reserve(2 * projectedColumns.size() + 2 + std::distance(asIt, asItEnd));
          for(auto const& projectedColumn : projectedColumns) {
            auto const& symbol =
                get<Symbol>(get<ComplexExpression>(projectedColumn).getArguments().at(0));
            newAsExprs.emplace_back(symbol);
            newAsExprs.emplace_back(symbol);
          }
          newAsExprs.emplace_back(std::move(name));
          newAsExprs.emplace_back(std::move(asFunc));
          newAsExprs.insert(newAsExprs.end(), asIt, asItEnd);
          projectedColumns.reserve(columns.size());
          std::transform(std::make_move_iterator(columns.begin()),
                         std::make_move_iterator(columns.end()),
                         std::back_inserter(projectedColumns),
                         [](auto&& column) { return std::forward<decltype(column)>(column); });
          return "Project"_(ComplexExpression("Table"_, std::move(projectedColumns)),
                            ComplexExpression("As"_, std::move(newAsExprs)));
        }
        auto dynamics = ExpressionArguments{};
        dynamics.emplace_back(std::move(name));
        dynamics.emplace_back(ComplexExpression("List"_, {}, {}, std::move(spans)));
        projectedColumns.emplace_back(ComplexExpression("Column"_, std::move(dynamics)));
      }
      // keep only the indices that are used down the pipeline
      for(auto&& column : columns) {
        auto const& columnExpr = get<ComplexExpression>(column);
        if(columnExpr.getHead() == "Index"_ && !columnExpr.getDynamicArguments().empty()) {
          if(usedTableSymbols(true).contains(get<Symbol>(columnExpr.getDynamicArguments()[0]))) {
            projectedColumns.emplace_back(std::move(column));
          }
        }
      }
      return ComplexExpression("Table"_, std::move(projectedColumns));
    };

    (*this)["Select"_] = [](ComplexExpression&& inputExpr) -> Expression {
      ExpressionArguments args = std::move(inputExpr).getArguments();
      auto it = std::make_move_iterator(args.begin());
      auto relation = boss::get<ComplexExpression>(std::move(*it));
      auto predExpr = std::move(*++it);
      if(properties().disableGather || !std::holds_alternative<Pred>(predExpr)) {
        // return unevaluated
        return "Select"_(std::move(relation), std::move(predExpr));
      }
      ExpressionSpanArguments previousPositionSpans{};
      if(relation.getHead().getName() == "Gather") {
        // nested Selection: extract the relation and the previous position list to merge
        auto [head, unused_, dynamics, spans] = std::move(relation).decompose();
        previousPositionSpans = (ExpressionSpanArguments)std::move(spans);
        relation = boss::get<ComplexExpression>(std::move(dynamics[0]));
      } else if(relation.getHead().getName() != "Table") {
        // return unevaluated
        return "Select"_(std::move(relation), std::move(predExpr));
      }
      auto predFunc = boss::get<Pred>(std::move(predExpr));
      auto columns = std::move(relation).getDynamicArguments();
      bool canEvaluateSelect = true;
      std::transform(
          std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
          columns.begin(), [&canEvaluateSelect](auto&& columnExpr) {
            auto column = get<ComplexExpression>(std::forward<decltype(columnExpr)>(columnExpr));
            auto [head, unused_, dynamics, spans] = std::move(column).decompose();
            auto list = get<ComplexExpression>(std::move(dynamics.at(1)));
            if(list.getHead() == "DictionaryEncodedList"_) {
              auto [unused1, unused2, listDynamics, unused3] = std::move(list).decompose();
              list = std::move(get<ComplexExpression>(listDynamics[0]));
            }
            list = transformDynamicsToSpans(std::move(list));
            auto [listHead, unusedStatics, unusedDynamics, listSpans] = std::move(list).decompose();
            // check if the span types are supported by ArrayFire
            for(auto const& span : listSpans) {
              if(!std::holds_alternative<Span<int32_t>>(span) &&
                 !std::holds_alternative<Span<int64_t>>(span) &&
                 !std::holds_alternative<Span<double_t>>(span) &&
                 !std::holds_alternative<Span<Pred>>(span) &&
                 !std::holds_alternative<Span<int32_t const>>(span) &&
                 !std::holds_alternative<Span<int64_t const>>(span) &&
                 !std::holds_alternative<Span<double_t const>>(span) &&
                 !std::holds_alternative<Span<Pred const>>(span)) {
                canEvaluateSelect = false;
                break;
              }
            }
            dynamics.at(1) = ComplexExpression(std::move(listHead), {}, {}, std::move(listSpans));
            return ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
          });
      auto createUnevaluatedOutput = [&]() {
        auto newRelation = ComplexExpression("Table"_, std::move(columns));
        if(!previousPositionSpans.empty()) {
          ExpressionArguments dynamics;
          dynamics.emplace_back(std::move(newRelation));
          newRelation = ComplexExpression("Gather"_, {}, std::move(dynamics),
                                          std::move(previousPositionSpans));
        }
        return "Select"_(std::move(newRelation), "Where"_(std::move(predFunc)));
      };
      if(!canEvaluateSelect) {
        // return unevaluated
        return createUnevaluatedOutput();
      }
      // iterate on each predicate partition
      // and, for each, generate a span for the position list
      ExpressionSpanArguments positionSpans{};
      auto prevPositionSpansIt = std::make_move_iterator(previousPositionSpans.begin());
      auto prevPositionSpansItEnd = std::make_move_iterator(previousPositionSpans.end());
      auto constexpr merge = false;        // keep the partitions
      auto constexpr transferToGPU = true; // the predicate columns need to be transferred
      while(auto predicate = predFunc(columns, merge, transferToGPU)) {
        auto bitArray = static_cast<af::array const&>(*predicate);
        auto positions = af::array(af::where(bitArray).as(s32));
        positions.eval();
        if(prevPositionSpansIt != prevPositionSpansItEnd) {
          auto prevPositions = static_cast<af::array const&>(
              std::get<boss::Span<Pred>>(std::move(*prevPositionSpansIt++)));
          af::sync(); // seems not guaranteed inside setIntersect, so the processing could fail
          positions = af::setIntersect(prevPositions, positions, true);
          positions.eval();
        }
        positionSpans.emplace_back(boss::Span<Pred>{std::move(positions)});
      }
      if(positionSpans.empty()) {
        // There was no span to process.
        // Most likely, we ran out of GPU memory.
        // Return the expression unevaluated
        return createUnevaluatedOutput();
      }
      ExpressionArguments dynamics;
      dynamics.emplace_back(ComplexExpression("Table"_, std::move(columns)));
      return ComplexExpression("Gather"_, {}, std::move(dynamics), std::move(positionSpans));
    };

    (*this)["Join"_] = [](ComplexExpression&& inputExpr) -> Expression {
      ExpressionArguments args = std::move(inputExpr).getArguments();
      auto it = std::make_move_iterator(args.begin());
      auto leftSideRelation = boss::get<ComplexExpression>(std::move(*it));
      auto rightSideRelation = boss::get<ComplexExpression>(std::move(*++it));
      auto predExpr = std::move(*++it);
      if(leftSideRelation.getHead().getName() != "Table" ||
         rightSideRelation.getHead().getName() != "Table" ||
         !std::holds_alternative<Pred>(predExpr)) {
        // return unevaluated
        return "Join"_(std::move(leftSideRelation), std::move(rightSideRelation),
                       std::move(predExpr));
      }
      auto leftSideColumns = std::move(leftSideRelation).getDynamicArguments();
      auto rightSideColumns = std::move(rightSideRelation).getDynamicArguments();
      // retrieve the expression-predicate (rather than working with the Pred function)
      auto unevaluatedPredExpr = boss::get<ComplexExpression>(
          (Expression)(boss::Expression)boss::get<Pred>(std::move(predExpr)));
      auto const& predHead = unevaluatedPredExpr.getHead();
      auto const& leftSideSymbol = get<Symbol>(unevaluatedPredExpr.getArguments()[0]);
      auto const& rightSideSymbol = get<Symbol>(unevaluatedPredExpr.getArguments()[1]);
      // check if we can find an index for the foreign key on the second relation
      auto foreignKeyIndexIt = std::find_if(
          rightSideColumns.begin(), rightSideColumns.end(), [&leftSideSymbol](auto const& column) {
            return get<ComplexExpression>(column).getHead() == "Index"_ &&
                   get<Symbol>(get<ComplexExpression>(column).getArguments()[0]) == leftSideSymbol;
          });
      if(predHead != "Equal"_ || filteredAttributes().contains(leftSideSymbol) ||
         foreignKeyIndexIt == rightSideColumns.end()) {
        // return unevaluated
        return "Join"_(ComplexExpression("Table"_, std::move(leftSideColumns)),
                       ComplexExpression("Table"_, std::move(rightSideColumns)),
                       std::move(unevaluatedPredExpr));
      }
      bool canEvaluateJoin = true;
      std::transform(
          std::make_move_iterator(leftSideColumns.begin()),
          std::make_move_iterator(leftSideColumns.end()), leftSideColumns.begin(),
          [&canEvaluateJoin](auto&& columnExpr) {
            auto column = get<ComplexExpression>(std::forward<decltype(columnExpr)>(columnExpr));
            auto [head, unused_, dynamics, spans] = std::move(column).decompose();
            auto list = get<ComplexExpression>(std::move(dynamics.at(1)));
            if(list.getHead() == "DictionaryEncodedList"_) {
              auto [unused1, unused2, listDynamics, unused3] = std::move(list).decompose();
              list = std::move(get<ComplexExpression>(listDynamics[0]));
            }
            list = transformDynamicsToSpans(std::move(list));
            auto [listHead, unusedStatics, unusedDynamics, listSpans] = std::move(list).decompose();
            // check if the span types are supported by ArrayFire
            for(auto const& span : listSpans) {
              if(!std::holds_alternative<Span<int32_t>>(span) &&
                 !std::holds_alternative<Span<int64_t>>(span) &&
                 !std::holds_alternative<Span<double_t>>(span) &&
                 !std::holds_alternative<Span<Pred>>(span) &&
                 !std::holds_alternative<Span<int32_t const>>(span) &&
                 !std::holds_alternative<Span<int64_t const>>(span) &&
                 !std::holds_alternative<Span<double_t const>>(span) &&
                 !std::holds_alternative<Span<Pred const>>(span)) {
                canEvaluateJoin = false;
                break;
              }
            }
            dynamics.at(1) = ComplexExpression(std::move(listHead), {}, {}, std::move(listSpans));
            return ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
          });
      std::transform(
          std::make_move_iterator(rightSideColumns.begin()),
          std::make_move_iterator(rightSideColumns.end()), rightSideColumns.begin(),
          [&canEvaluateJoin](auto&& columnExpr) {
            auto column = get<ComplexExpression>(std::forward<decltype(columnExpr)>(columnExpr));
            auto [head, unused_, dynamics, spans] = std::move(column).decompose();
            auto list = get<ComplexExpression>(std::move(dynamics.at(1)));
            if(list.getHead() == "DictionaryEncodedList"_) {
              auto [unused1, unused2, listDynamics, unused3] = std::move(list).decompose();
              list = std::move(get<ComplexExpression>(listDynamics[0]));
            }
            list = transformDynamicsToSpans(std::move(list));
            auto [listHead, unusedStatics, unusedDynamics, listSpans] = std::move(list).decompose();
            // check if the span types are supported by ArrayFire
            for(auto const& span : listSpans) {
              if(!std::holds_alternative<Span<int32_t>>(span) &&
                 !std::holds_alternative<Span<int64_t>>(span) &&
                 !std::holds_alternative<Span<double_t>>(span) &&
                 !std::holds_alternative<Span<Pred>>(span) &&
                 !std::holds_alternative<Span<int32_t const>>(span) &&
                 !std::holds_alternative<Span<int64_t const>>(span) &&
                 !std::holds_alternative<Span<double_t const>>(span) &&
                 !std::holds_alternative<Span<Pred const>>(span)) {
                canEvaluateJoin = false;
                break;
              }
            }
            dynamics.at(1) = ComplexExpression(std::move(listHead), {}, {}, std::move(listSpans));
            return ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
          });
      if(!canEvaluateJoin) {
        // return unevaluated
        return "Join"_(ComplexExpression("Table"_, std::move(leftSideColumns)),
                       ComplexExpression("Table"_, std::move(rightSideColumns)),
                       std::move(unevaluatedPredExpr));
      }
      // get a function to retrieve the index rather than using the predicate
      auto pullIndexFunc = createLambdaArgument(leftSideSymbol);
      // retrieve all the left side partitions as single (combined) partitions
      // + initialise left-side join columns
      std::vector<af::array> leftSideArrays;
      ExpressionArguments joinedColumns;
      for(auto const& column : leftSideColumns) {
        auto const& symbol = get<Symbol>(get<ComplexExpression>(column).getArguments()[0]);
        if(get<ComplexExpression>(column).getHead() == "Index"_) {
          // check if the index can be dropped (if from the FK of the right-side table)
          if(symbol == rightSideSymbol) {
            continue;
          }
        } else if(symbol == leftSideSymbol && !usedTableSymbols(false).contains(symbol)) {
          // skip the left side key (when not used downstream)
          continue;
        }
        auto retrieveSpanFunc = createLambdaArgument(symbol);
        auto constexpr merge = true;         // merge the partitions (required for the lookup)
        auto constexpr transferToGPU = true; // all the columns need to be transferred
        auto span = retrieveSpanFunc(leftSideColumns, merge, transferToGPU);
        if(!span) {
          // There is no span to process.
          // Most likely, we ran out of GPU memory.
          // Return the expression as unevaluated
          return "Join"_(ComplexExpression("Table"_, std::move(leftSideColumns)),
                         ComplexExpression("Table"_, std::move(rightSideColumns)),
                         std::move(unevaluatedPredExpr));
        }
        leftSideArrays.emplace_back(*span);
        ExpressionArguments args;
        args.reserve(2);
        args.emplace_back(symbol);
        args.emplace_back("List"_());
        joinedColumns.emplace_back(
            ComplexExpression(get<ComplexExpression>(column).getHead(), std::move(args)));
      }
      // iterate on the index' partitions
      auto constexpr merge =
          true; // merge the partitions, so the left-side arrays are consumed after use
      auto constexpr transferToGPU = true; // the right-side key need to be transferred
      auto foreignKeyIndexSpan = pullIndexFunc(rightSideColumns, merge, transferToGPU);
      if(foreignKeyIndexSpan) {
        auto whereArray = toArrayFireOrConstant(*foreignKeyIndexSpan);
        auto leftSideArrayIt = std::make_move_iterator(leftSideArrays.begin());
        for(auto& columnExpr : joinedColumns) {
          // for each column, filter the left-side span with the index
          // and add to the joined spans
          auto column = get<ComplexExpression>(std::move(columnExpr));
          auto [head, unused_, dynamics, spans] = std::move(column).decompose();
          auto list = get<ComplexExpression>(std::move(dynamics.at(1)));
          auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
          auto leftSideArray = std::move(*leftSideArrayIt++); // will be consume and released
                                                              // at the end of the iteration
                                                              // so we save GPU memory
          auto result = af::lookup(leftSideArray, whereArray);
          auto spanResult = Span<Pred>(std::move(result));
          auto splitSpans = spanResult.resplitIntoPartitionsToMatch(*foreignKeyIndexSpan);
          listSpans.insert(listSpans.end(), std::make_move_iterator(splitSpans.begin()),
                           std::make_move_iterator(splitSpans.end()));
          dynamics.at(1) = ComplexExpression(std::move(listHead), {}, std::move(listDynamics),
                                             std::move(listSpans));
          columnExpr =
              ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
        }
      } else {
        // There was no span to process.
        // Most likely, we ran out of GPU memory.
        // Return the expression as unevaluated
        return "Join"_(ComplexExpression("Table"_, std::move(leftSideColumns)),
                       ComplexExpression("Table"_, std::move(rightSideColumns)),
                       std::move(unevaluatedPredExpr));
      }
      // add the right-side columns as-is
      for(auto&& columnExpr : rightSideColumns) {
        auto column = get<ComplexExpression>(std::move(columnExpr));
        auto [head, unused_, dynamics, spans] = std::move(column).decompose();
        if(head == "Index"_) {
          // check if the index can be dropped (if from the FK of the left-side table)
          if(get<Symbol>(dynamics[0]) == leftSideSymbol) {
            continue;
          }
        }
        joinedColumns.emplace_back(
            ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
      }
      return ComplexExpression("Table"_, std::move(joinedColumns));
    };
  }

private:
  template <typename T> static T const& toArrayFireOrConstant(T& arg) { return arg; }
  static af::array const& toArrayFireOrConstant(Span<Pred>& arg) {
    return static_cast<af::array const&>(arg);
  }

  template <unsigned int MIN_ARGS, unsigned int MAX_ARGS = (0U - 1U), typename T, typename F>
  static Pred createLambdaExpression(ComplexExpressionWithStaticArguments<T>&& e, F&& f) {
    assert(e.getSpanArguments().empty());                   // NOLINT
    assert(1 + e.getDynamicArguments().size() >= MIN_ARGS); // NOLINT
    assert(1 + e.getDynamicArguments().size() <= MAX_ARGS); // NOLINT
    if constexpr(MAX_ARGS == 1) {
      auto pred = [pred1 = createLambdaArgument(get<0>(e.getStaticArguments())),
                   f](ExpressionArguments& columns, bool merge,
                      bool /*transferToGPU*/) mutable -> std::optional<Span<Pred>> {
        auto arg1 = pred1(columns, merge, true); // always transfer if computation is needed
        if(!arg1) {
          return {};
        }
        return (Span<Pred>)f(toArrayFireOrConstant(*arg1));
      };
      return {std::move(pred), toBOSSExpression(std::move(e))};
    } else if constexpr(!std::is_same_v<T, Pred> && !std::is_same_v<T, Symbol> && MAX_ARGS == 2) {
      auto pred = std::visit(
          [&e, &f](auto&& arg) -> Pred::Function {
            return [pred1 = createLambdaArgument(get<0>(e.getStaticArguments())),
                    pred2 = createLambdaArgument(arg),
                    f](ExpressionArguments& columns, bool merge,
                       bool /*transferToGPU*/) mutable -> std::optional<Span<Pred>> {
              auto arg1 = pred1(columns, merge, true); // always transfer if computation is needed
              auto arg2 = pred2(columns, merge, true); // always transfer if computation is needed
              if(!arg1 || !arg2) {
                return {};
              }
              return (Span<Pred>)f(toArrayFireOrConstant(*arg1), toArrayFireOrConstant(*arg2));
            };
          }, // NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
          e.getDynamicArguments().at(0));
      return {std::move(pred), toBOSSExpression(std::move(e))};
    } else {
      auto pred = std::accumulate(
          e.getDynamicArguments().begin(), e.getDynamicArguments().end(),
          (Pred::Function &&) createLambdaArgument(get<0>(e.getStaticArguments())),
          [&f](auto&& acc, auto const& e) -> Pred::Function {
            return std::visit(
                [&e, &f, &acc](auto&& arg) -> Pred::Function {
                  auto pred2 = createLambdaArgument(arg);
                  return
                      [acc, pred2, f](ExpressionArguments& columns, bool merge,
                                      bool /*transferToGPU*/) mutable -> std::optional<Span<Pred>> {
                        auto arg1 =
                            acc(columns, merge, true); // always transfer if computation is needed
                        auto arg2 =
                            pred2(columns, merge, true); // always transfer if computation is needed
                        if(!arg1 || !arg2) {
                          return {};
                        }
                        return (Span<Pred>)f(toArrayFireOrConstant(*arg1),
                                             toArrayFireOrConstant(*arg2));
                      };
                },
                e);
          });
      return {std::move(pred), toBOSSExpression(std::move(e))};
    }
  } // NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
  template <unsigned int MIN_ARGS, unsigned int MAX_ARGS = (0U - 1U), typename T1, typename T2,
            typename F>
  static Pred createLambdaExpression(ComplexExpressionWithStaticArguments<T1, T2>&& e, F&& f) {
    assert(e.getSpanArguments().empty());                   // NOLINT
    assert(2 + e.getDynamicArguments().size() >= MIN_ARGS); // NOLINT
    assert(2 + e.getDynamicArguments().size() <= MAX_ARGS); // NOLINT
    auto predA = createLambdaArgument(get<0>(e.getStaticArguments()));
    auto predB = createLambdaArgument(get<1>(e.getStaticArguments()));
    Pred::Function pred1 = [predA, predB,
                            f](ExpressionArguments& columns, bool merge,
                               bool /*transferToGPU*/) mutable -> std::optional<Span<Pred>> {
      auto arg1 = predA(columns, merge, true); // always transfer if computation is needed
      auto arg2 = predB(columns, merge, true); // always transfer if computation is needed
      if(!arg1 || !arg2) {
        return {};
      }
      return (Span<Pred>)f(toArrayFireOrConstant(*arg1), toArrayFireOrConstant(*arg2));
    };
    if constexpr(MAX_ARGS == 2) {
      return {pred1, toBOSSExpression(std::move(e))};
    } else {
      auto pred = std::accumulate(
          e.getDynamicArguments().begin(), e.getDynamicArguments().end(), pred1,
          [&f](auto&& acc, auto const& e) -> Pred::Function {
            return std::visit(
                [&e, &f, &acc](auto&& arg) -> Pred::Function {
                  auto pred2 = createLambdaArgument(arg);
                  return
                      [acc, pred2, f](ExpressionArguments& columns, bool merge,
                                      bool /*transferToGPU*/) mutable -> std::optional<Span<Pred>> {
                        auto arg1 =
                            acc(columns, merge, true); // always transfer if computation is needed
                        auto arg2 =
                            pred2(columns, merge, true); // always transfer if computation is needed
                        if(!arg1 || !arg2) {
                          return {};
                        }
                        return (Span<Pred>)f(toArrayFireOrConstant(*arg1),
                                             toArrayFireOrConstant(*arg2));
                      };
                },
                e);
          });
      return {std::move(pred), toBOSSExpression(std::move(e))};
    }
  }

  template <typename ArgType> static auto createLambdaArgument(ArgType const& arg) {
    if constexpr(NumericType<ArgType>) {
      return [arg](ExpressionArguments& /*columns*/, bool /*merge*/,
                   bool /*transferToGPU*/) -> std::optional<ArgType> { return arg; };
    } else {
      throw std::runtime_error("unsupported argument type in predicate");
      return [](ExpressionArguments& /*columns*/, bool /*merge*/,
                bool /*transferToGPU*/) -> std::optional<bool> { return {}; };
    }
  }

  static Pred::Function createLambdaArgument(Pred const& arg) {
    return [f = static_cast<Pred::Function const&>(arg)](ExpressionArguments& columns, bool merge,
                                                         bool transferToGPU) {
      return f(columns, merge, transferToGPU);
    };
  }

  static Pred::Function createLambdaArgument(Symbol const& arg) {
    return [arg, index = 0U - 1U, partitions = std::vector<Span<Pred>>(),
            initialised = false](ExpressionArguments& columns, bool merge,
                                 bool transferToGPU) mutable -> std::optional<Span<Pred>> {
      ++index;
      // search for column matching the symbol in the relation
      for(auto& columnExpr : columns) {
        auto& column = get<ComplexExpression>(columnExpr);
        if(get<Symbol>(column.getArguments().at(0)) != arg) {
          continue;
        }
        if(!initialised) {
          initialised = true;
          // Extract the index-th span
          // Decompose and recompose so spans can be swapped (see below)
          auto [head, unused_, dynamics, spans] = std::move(column).decompose();
          auto& list = get<ComplexExpression>(dynamics.at(1));
          auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
          uint64_t requiredMemory = 0;
          void* startPointer = nullptr;
          ExpressionSpanArguments newListSpans;
          for(auto&& span : listSpans) {
            auto afArray = std::visit(
                [&newListSpans, &arg, &requiredMemory, &transferToGPU,
                 &startPointer]<typename T>(Span<T>&& typedSpan) -> Span<Pred> {
                  if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                               std::is_same_v<T, double_t> || std::is_same_v<T, std::string> ||
                               std::is_same_v<T, int32_t const> ||
                               std::is_same_v<T, int64_t const> ||
                               std::is_same_v<T, double_t const> ||
                               std::is_same_v<T, std::string const> || std::is_same_v<T, Pred>) {
                // check only for first span due to combining the partitions
#ifdef ABLATION_NO_FAST_PATH
                    void* pointer =
                        const_cast<void*>( // NOLINT(cppcoreguidelines-pro-type-const-cast)
                            (void const*)typedSpan.baseBegin());
#else
                    void* pointer =
                        const_cast<void*>( // NOLINT(cppcoreguidelines-pro-type-const-cast)
                            (void const*)typedSpan.begin());
#endif // ABLATION_NO_FAST_PATH
                    if(transferToGPU && pointer != nullptr) {
                      if(cachedGPUArrays().contains(pointer) ||
                         state().plannedMovedColumns.contains(pointer)) {
                        transferToGPU = false; // also will apply to all other spans
                                               // (no need to check again for each of them)
                      } else {
                        // keep track of memory usage required on the GPU
                        if constexpr(std::is_same_v<T, Pred>) {
                          requiredMemory += typedSpan.bytesToMoveToGPU();
                        } else {
                          requiredMemory += sizeof(T) * typedSpan.size();
                        }
                        if(startPointer == nullptr) {
                          startPointer = pointer;
                        }
                      }
                    }
                    // since copying the span with guarantee to keep it in scope is not possible,
                    // we need to swap it (the new span converted from Span<Pred> has this
                    // guarantee)
                    auto afArray = Span<Pred>(std::move(typedSpan), arg.getName());
                    newListSpans.emplace_back(static_cast<boss::Span<T>>(afArray));
                    return std::move(afArray);
                  } else {
                    throw std::runtime_error("unsupported column type in predicate");
                  }
                },
                std::move(span));
            partitions.emplace_back(std::move(afArray));
          }
          list = ComplexExpression(std::move(listHead), {}, std::move(listDynamics),
                                   std::move(newListSpans));
          column = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
          if(requiredMemory > 0) {
            auto currentGPUMemoryUsage =
                Span<Pred>::currentGPUMemoryUsage() + state().plannedGPUMemoryUsage;
            if(currentGPUMemoryUsage + requiredMemory > properties().MaxGPUMemoryCache) {
              // cannot anymore be performed on the GPU
              // return empty partitions and let the operators be unevaluated
#ifdef LOGGING_TRANSFER_TO_GPU
              std::cout << "GPU: cannot reserve " << requiredMemory << " for " << arg.getName()
                        << std::endl;
#endif
              partitions.clear();
              return {};
            }
            if(startPointer != nullptr) {
              state().plannedMovedColumns.insert(startPointer);
            }
            state().plannedGPUMemoryUsage += requiredMemory;
#ifdef LOGGING_TRANSFER_TO_GPU
            std::cout << "GPU: reserve " << requiredMemory << " for " << arg.getName() << std::endl;
            std::cout << "GPU: remaining memory " << state().plannedGPUMemoryUsage << "/"
                      << properties().MaxGPUMemoryCache << std::endl;
#endif
          }
        }
        if(transferToGPU && merge && index == 0) {
          // combine the spans on the GPU,
          // so the join operator implementation can lookup across partitions
          auto combined = Span<Pred>::combineOnGPU(std::move(partitions));
          partitions.clear();
          return std::move(combined);
        }
        if(index >= partitions.size()) {
          return {};
        }
        return std::move(partitions[index]);
      }
      throw std::runtime_error("in predicate: unknown symbol \"" + arg.getName() + "\"_");
    };
  }
};

static void findUsedTableSymbols(Expression const& expr, bool symbolAsIndex) {
  visit(boss::utilities::overload(
            [&symbolAsIndex](ComplexExpression const& e) {
              for(auto const& arg : e.getDynamicArguments()) {
                findUsedTableSymbols(arg, symbolAsIndex);
              }
            },
            [&symbolAsIndex](Symbol const& s) { usedTableSymbols(symbolAsIndex).insert(s); },
            [](auto const& /*unused*/) {}),
        expr);
}

static Expression evaluateInternal(Expression&& expr) {
  static OperatorMap operators;
  return visit(
      boss::utilities::overload(
          [](ComplexExpression&& e) -> Expression {
            auto head = e.getHead();
            auto it = operators.find(head);
            if(it != operators.end()) {
              auto findUsedTableSymbolsAndEvaluate = [&](auto const& predicate,
                                                         bool symbolAsIndex) {
                // making on purpose a copy of the previous symbols
                auto oldSymbols = usedTableSymbols(symbolAsIndex);
                // update the symbols
                findUsedTableSymbols(predicate, symbolAsIndex);
                // evaluate
                auto result = it->second(std::move(e));
                // load back previous symbols
                std::swap(oldSymbols, usedTableSymbols(symbolAsIndex));
                return std::move(result);
              };
              if(head == "Select"_ || head == "Project"_) {
                if(e.getDynamicArguments().size() > 1) {
                  return findUsedTableSymbolsAndEvaluate(e.getDynamicArguments()[1], false);
                }
              }
              if(head == "Join"_) {
                if(e.getDynamicArguments().size() > 2) {
                  return findUsedTableSymbolsAndEvaluate(e.getDynamicArguments()[2], true);
                }
              }
              return it->second(std::move(e));
            }
            // at least evaluate all the arguments
            auto [_, unused_, dynamics, spans] = std::move(e).decompose();
            std::transform(
                std::make_move_iterator(dynamics.begin()), std::make_move_iterator(dynamics.end()),
                dynamics.begin(),
                [](auto&& arg) { return evaluateInternal(std::forward<decltype(arg)>(arg)); });
            return ComplexExpression{std::move(head), {}, std::move(dynamics), std::move(spans)};
          },
          [](auto&& e) -> Expression { return std::forward<decltype(e)>(e); }),
      std::forward<decltype(expr)>(expr));
}

static boss::Expression injectDebugInfoToSpans(boss::Expression&& expr) {
  return std::visit(
      boss::utilities::overload(
          [&](boss::ComplexExpression&& e) -> boss::Expression {
            auto [head, unused_, dynamics, spans] = std::move(e).decompose();
            boss::ExpressionArguments debugDynamics;
            debugDynamics.reserve(dynamics.size() + spans.size());
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           std::back_inserter(debugDynamics), [](auto&& arg) {
                             return injectDebugInfoToSpans(std::forward<decltype(arg)>(arg));
                           });
            std::transform(
                std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
                std::back_inserter(dynamics), [](auto&& span) {
                  return std::visit(
                      []<typename T>(Span<T>&& typedSpan) -> boss::Expression {
                        return boss::ComplexExpressionWithStaticArguments<std::string, int64_t>(
                            "Span"_, {typeid(T).name(), typedSpan.size()});
                      },
                      std::forward<decltype(span)>(span));
                });
            return boss::ComplexExpression(std::move(head), {}, std::move(debugDynamics));
          },
          [](auto&& otherTypes) -> boss::Expression { return otherTypes; }),
      std::move(expr));
}

static boss::Expression evaluate(boss::Expression&& expr) {
  try {
    auto output = properties().copyDataIn ? evaluateInternal(utilities::deepCopy(expr))
                                          : evaluateInternal(std::move(expr));
    filteredAttributes().clear();
    return properties().copyDataOut ? utilities::deepCopy(toBOSSExpression(std::move(output)))
                                    : toBOSSExpression(std::move(output));
  } catch(std::exception const& e) {
    boss::ExpressionArguments args;
    args.reserve(2);
    args.emplace_back(injectDebugInfoToSpans(std::move(expr)));
    args.emplace_back(std::string{e.what()});
    return boss::ComplexExpression{"ErrorWhenEvaluatingExpression"_, std::move(args)};
  }
}

extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
  return new BOSSExpression{.delegate = evaluate(std::move(e->delegate))};
};

extern "C" void reset() {}
