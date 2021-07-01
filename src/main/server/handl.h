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

#ifndef TRITON_HANDL_H
#define TRITON_HANDL_H

#include <seastar/http/httpd.hh>
#include <seastar/core/seastar.hh>

using namespace seastar;
using namespace httpd;

class handl : public httpd::handler_base {
public:
  virtual future<std::unique_ptr<reply> > handle(const sstring& path,
                                                 std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    rep->_content = "hello";
    rep->done("html");
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
  }
};

#endif//TRITON_HANDL_H
