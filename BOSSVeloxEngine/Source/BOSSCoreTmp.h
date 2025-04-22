// Data structures and functions in this file are candidates to be moved into BOSS core.

#pragma once

#include <functional>
#include <unordered_map>

namespace boss::engines::velox {
    
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

private:
  struct Info {
    std::function<void(void)> destructor;
    unsigned int counter = 0;

    explicit Info(std::function<void(void)>&& f) : destructor(std::move(f)) {}
  };

  std::unordered_map<void*, Info> map;
};

static SpanReferenceCounter spanReferenceCounter;

} // namespace boss::engines::velox
