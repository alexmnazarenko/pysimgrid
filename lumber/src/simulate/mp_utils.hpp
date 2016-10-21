// A. Nazarenko 2016

#pragma once

#include <utility>

namespace simulate {
/**
 * Horrible metaprogramming stuff to simplify work with schedule derived classes.
 */
namespace mp {

/** Simple static typelist */
template <class Head, typename... Types>
struct Typelist {
  typedef Head head; ///< list head type
  typedef Typelist<Types...> tail; ///< li

  typedef std::true_type has_next;
};


/** Specialization for single value */
template <typename Head>
struct Typelist<Head> {
    typedef Head head;           ///< last type in list
    typedef std::nullptr_t tail; ///< guard type

    typedef std::false_type has_next;
};


/** Typelist traverse with custom visitor class */
template<class Typelist, class Proceed=std::true_type>
struct Visit {
  template<class Visitor> inline
  static void visit(Visitor& action) {
    if (action.template visit<class Typelist::head>()) {
      Visit<class Typelist::tail, class Typelist::has_next>::visit(action);
    }
  }
};


/** Specialization stopping recursive call */
template<class Typelist>
struct Visit<Typelist, std::false_type> {
  template<class Visitor> inline
  static void visit(Visitor& action) {}
};


/** User-friendly interface - create Visitor(Args...), traverse typelist and return it*/
template<class Visitor, class Typelist, class... Args> inline
Visitor apply_visitor(Args&&... args) {
  Visitor result{std::forward<Args>(args)...};
  Visit<Typelist>::visit(result);
  return result;
}


}
}
