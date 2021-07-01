/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TRITON_STRINGUTILS_H
#define TRITON_STRINGUTILS_H

#include <cctype>
#include <vector>
#include <memory>
#include <sstream>
#include <type_traits>
#include <string_view>
#include <algorithm>
#include <iterator>

namespace triton {

  template <typename Range, typename Value = typename Range::value_type>
  std::string join(Range const& elements, const char *const delimiter) {
    std::ostringstream os;
    auto b = begin(elements);
    auto e = end(elements);

    if (b != e) {
      std::copy(b, prev(e), std::ostream_iterator<Value>(os, delimiter));
      b = prev(e);
    }
    if (b != e) {
      os << *b;
    }

    return os.str();
  }

} // namespace triton

#endif//TRITON_STRINGUTILS_H
