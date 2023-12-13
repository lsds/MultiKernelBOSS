#pragma once

#include "Expression.hpp"
#include <algorithm>

namespace boss::algorithm {
template <typename Container, typename Visitor> void visitEach(Container c, Visitor v) {
  using std::for_each;
  for_each(c.begin(), c.end(),
           [&v](auto&& item) { visit([&v](auto&& item) { return v(item); }, item); });
}

template <typename Container, typename Init, typename Visitor>
auto visitAccumulate(Container c, Init i, Visitor v) {
  using std::accumulate;
  return accumulate(c.begin(), c.end(), i, [&v](auto&& state, auto&& item) {
    return visit(
        [&state, &v](auto&& item) {
          return v(::std::forward<decltype(state)>(state), ::std::forward<decltype(item)>(item));
        },
        ::std::forward<decltype(item)>(item));
  });
}

template <typename Container, typename TransformVisitor, typename Init, typename AccumulateVisitor>
auto visitTransformAccumulate(Container c, TransformVisitor t, Init i, AccumulateVisitor v) {
  using std::accumulate;
  return accumulate(c.begin(), c.end(), i, [&v, &t](auto&& state, auto&& item) {
    return visit(
        [&state, &v](auto&& item) { return v(::std::forward<decltype(state)>(state), item); },
        ::std::visit([&t](auto&& item) { return t(::std::forward<decltype(item)>(item)); },
                     ::std::forward<decltype(item)>(item)));
  });
}

template <typename Container, typename TransformVisitor>
auto visitTransform(Container& c, TransformVisitor t) {
  using std::transform;
  return transform(c.begin(), c.end(), c.begin(), [&t](auto&& item) {
    return visit([&t](auto&& item) { return t(::std::forward<decltype(item)>(item)); },
                 ::std::forward<decltype(item)>(item));
  });
}

} // namespace boss::algorithm
