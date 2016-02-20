// A. Nazarenko 2016

#pragma once

#include <utility>

namespace simulate {
/**
 * Horrible metaprogramming stuff to simplify work with schedule derived classes.
 */
namespace mp {

template <class Head, typename... Types>
struct Typelist {
  typedef Head head;
  typedef Typelist<Types...> tail;

  typedef std::true_type has_next;
};


/// Specialization for single value
template <typename Head>
struct Typelist<Head> {
    typedef Head head;
    typedef std::nullptr_t tail;

    typedef std::false_type has_next;
};


template<class Typelist, class Proceed=std::true_type>
struct Visit {
  template<class Visitor>
  static void visit(Visitor& action) {
    if (action.template visit<class Typelist::head>()) {
      Visit<class Typelist::tail, class Typelist::has_next>::visit(action);
    }
  }
};


template<class Typelist>
struct Visit<Typelist, std::false_type> {
  template<class Visitor>
  static void visit(Visitor& action) {}
};


template<class Visitor, class Typelist, class... Args>
Visitor apply_visitor(Args&&... args) {
  Visitor result{std::forward<Args>(args)...};
  Visit<Typelist>::visit(result);
  return result;
}


}
}
