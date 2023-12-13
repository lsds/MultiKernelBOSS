#pragma once

#include "Algorithm.hpp"
#include "BOSS.hpp"
#include "Engine.hpp"
#include "Expression.hpp"
#include "ExpressionUtilities.hpp"
#include "Utilities.hpp"

#ifndef _WIN32
#include <dlfcn.h>
#else
#include <filesystem>
#define NOMINMAX // max macro in minwindef.h interfering with std::max...
#include <windows.h>
constexpr static int RTLD_NOW = 0;
constexpr static int RTLD_NODELETE = 0;
static void* dlopen(LPCSTR lpLibFileName, int /*flags*/) {
  void* libraryPtr = LoadLibrary(lpLibFileName);
  if(libraryPtr != nullptr) {
    return libraryPtr;
  }
  // if it failed to load the standard way (searching dependent dlls in the exe path)
  // try one more time, with loading the dependent dlls from the dll path
  auto filepath = ::std::filesystem::path(lpLibFileName);
  if(filepath.is_absolute()) {
    return LoadLibraryEx(lpLibFileName, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
  } else {
    auto absFilepath = ::std::filesystem::absolute(filepath).string();
    LPCSTR lpAbsFileName = absFilepath.c_str();
    return LoadLibraryEx(lpAbsFileName, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
  }
}
static auto dlclose(void* hModule) {
  auto resetFunction = GetProcAddress((HMODULE)hModule, "reset");
  if(resetFunction != NULL) {
    (*reinterpret_cast<void (*)()>(resetFunction))();
  }
  return FreeLibrary((HMODULE)hModule);
}
static auto dlerror() {
  auto errorCode = GetLastError();
  LPSTR pBuffer = NULL;
  auto msg = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS |
                               FORMAT_MESSAGE_ALLOCATE_BUFFER,
                           NULL, errorCode, 0, (LPSTR)&pBuffer, 0, NULL);
  if(msg > 0) {
    // Assign buffer to smart pointer with custom deleter so that memory gets released
    // in case String's constructor throws an exception.
    auto deleter = [](void* p) { ::LocalFree(p); };
    ::std::unique_ptr<TCHAR, decltype(deleter)> ptrBuffer(pBuffer, deleter);
    return "(" + ::std::to_string(errorCode) + ") " + ::std::string(ptrBuffer.get(), msg);
  }
  return ::std::to_string(errorCode);
}
static void* dlsym(void* hModule, LPCSTR lpProcName) {
  return GetProcAddress((HMODULE)hModule, lpProcName);
}
#endif // _WIN32

#include <algorithm>
#include <iterator>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <variant>

namespace boss {
namespace engines {
namespace {

class BootstrapEngine : public boss::Engine {

  struct LibraryAndEvaluateFunction {
    void *library, *evaluateFunction;
  };

  struct LibraryCache : private ::std::unordered_map<::std::string, LibraryAndEvaluateFunction> {
    LibraryAndEvaluateFunction const& at(::std::string const& libraryPath) {
      if(count(libraryPath) == 0) {
        const auto* n = libraryPath.c_str();
        if(auto* library = dlopen(n, RTLD_NOW | RTLD_NODELETE)) { // NOLINT(hicpp-signed-bitwise)
          if(auto* sym = dlsym(library, "evaluate")) {
            emplace(libraryPath, LibraryAndEvaluateFunction{library, sym});
          } else {
            throw ::std::runtime_error("library \"" + libraryPath +
                                       "\" does not provide an evaluate function: " + dlerror());
          }
        } else {
          throw ::std::runtime_error("library \"" + libraryPath +
                                     "\" could not be loaded: " + dlerror());
        }
      };
      return unordered_map::at(libraryPath);
    }
    bool erase(::std::string const& libraryPath) {
      auto it = find(libraryPath);
      if(it != end()) {
        dlclose(it->second.library);
        unordered_map::erase(it);
        return true;
      }
      return false;
    }
    ~LibraryCache() {
      for(const auto& [name, library] : *this) {
        dlclose(library.library);
      }
    }

    LibraryCache() = default;
    LibraryCache(LibraryCache const&) = delete;
    LibraryCache(LibraryCache&&) = delete;
    LibraryCache& operator=(LibraryCache const&) = delete;
    LibraryCache& operator=(LibraryCache&&) = delete;
  } libraries;

  ::std::vector<::std::string> defaultEngine = {};

  ::std::unordered_map<boss::Symbol,
                       ::std::function<boss::Expression(boss::ComplexExpression&&)>> const
      registeredOperators{
          {boss::Symbol("EvaluateInEngines"),
           [this](auto&& e) -> boss::Expression {
             auto symbols = ::std::vector<BOSSExpression* (*)(BOSSExpression*)>();
             auto&& args = get<ComplexExpression>(e.getArguments().at(0)).getArguments();
             ::std::for_each(args.begin(), args.end(), [this, &e, &symbols](auto&& enginePath) {
               symbols.push_back(reinterpret_cast<BOSSExpression* (*)(BOSSExpression*)>(
                   libraries.at(get<::std::string>(enginePath)).evaluateFunction));
             });
             ::std::for_each(
                 ::std::make_move_iterator(::std::next(
                     e.getArguments().begin())), // Note: first argument is the engine path
                 ::std::make_move_iterator(::std::prev(e.getArguments().end())),
                 [&symbols](auto&& argument) {
                   auto* wrapper = new BOSSExpression{::std::forward<decltype(argument)>(argument)};
                   for(auto sym : symbols) {
                     auto* oldWrapper = wrapper;
                     wrapper = (sym(wrapper));
                     freeBOSSExpression(oldWrapper);
                   }
                   freeBOSSExpression(wrapper);
                 });

             auto* r = new BOSSExpression{*::std::prev(e.getArguments().end())};
             for(auto sym : symbols) {
               auto* oldWrapper = r;
               r = sym(r);
               freeBOSSExpression(oldWrapper);
             }
             auto result = ::std::move(r->delegate);
             freeBOSSExpression(r); // NOLINT
             return ::std::move(result);
           }},
          {boss::Symbol("ReleaseEngines"),
           [this](auto&& e) -> boss::Expression {
             auto&& args = get<ComplexExpression>(e.getArguments().at(0)).getArguments();
             return ::std::accumulate(
                 args.begin(), args.end(), true, [this](bool success, auto&& enginePath) {
                   return libraries.erase(get<::std::string>(enginePath)) && success;
                 });
           }},
          {boss::Symbol("SetDefaultEnginePipeline"), [this](auto&& expression) -> boss::Expression {
             algorithm::visitEach(expression.getArguments(), [this](auto&& engine) {
               if constexpr(::std::is_same_v<::std::decay_t<decltype(engine)>, ::std::string>) {
                 defaultEngine.push_back(engine);
               }
             });
             return "okay";
           }}};
  bool isBootstrapCommand(boss::Expression const& expression) {
    return visit(utilities::overload(
                     [this](boss::ComplexExpression const& expression) {
                       return registeredOperators.count(expression.getHead()) > 0;
                     },
                     [](auto const& /* unused */
                     ) { return false; }),
                 expression);
  }

public:
  BootstrapEngine() = default;
  ~BootstrapEngine() = default;
  BootstrapEngine(BootstrapEngine const&) = delete;
  BootstrapEngine(BootstrapEngine&&) = delete;
  BootstrapEngine& operator=(BootstrapEngine const&) = delete;
  BootstrapEngine& operator=(BootstrapEngine&&) = delete;

  auto evaluateArguments(boss::ComplexExpression&& expr) {
    ::std::transform(::std::make_move_iterator(begin(expr.getArguments())),
                     ::std::make_move_iterator(end(expr.getArguments())),
                     begin(expr.getArguments()),
                     [&](auto&& e) { return evaluate(::std::forward<decltype(e)>(e), false); });
    return ::std::move(expr);
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  boss::Expression evaluate(boss::Expression&& e, bool isRootExpression = true) {
    using boss::utilities::operator""_;

    auto wrappedE =
        isRootExpression && !defaultEngine.empty() && !isBootstrapCommand(e)
            ? "EvaluateInEngines"_(
                  "List"_(Span<::std::string>(defaultEngine.data(), defaultEngine.size(), nullptr)),
                  std::move(e))
            : std::move(e);
    return ::std::visit(boss::utilities::overload(
                            [this](boss::ComplexExpression&& unevaluatedE) -> boss::Expression {
                              if(registeredOperators.count(unevaluatedE.getHead()) == 0) {
                                return ::std::move(unevaluatedE);
                              }
                              auto const& op = registeredOperators.at(unevaluatedE.getHead());
                              return op(evaluateArguments(::std::move(unevaluatedE)));
                            },
                            [](auto&& e) -> boss::Expression { return e; }),
                        ::std::forward<boss::Expression>(wrappedE));
  }
};
} // namespace
} // namespace engines

} // namespace boss
