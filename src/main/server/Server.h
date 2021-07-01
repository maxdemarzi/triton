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

#ifndef TRITON_SERVER_H
#define TRITON_SERVER_H

#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

using namespace seastar;
using namespace httpd;
using namespace triton;

class Server {

public:
  static inline const seastar::sstring PROPERTY = sstring ("property");
  static inline const seastar::sstring TYPE = sstring ("type");
  static inline const seastar::sstring KEY = sstring ("key");
  static inline const seastar::sstring ID = sstring ("id");
  static inline const seastar::sstring ID2 = sstring ("id2");
  static inline const seastar::sstring TYPE2 = sstring ("type2");
  static inline const seastar::sstring KEY2 = sstring ("key2");
  static inline const seastar::sstring REL_TYPE = sstring ("rel_type");
  static inline const seastar::sstring OPTIONS = sstring ("options");

  static bool validate_parameter(const seastar::sstring& parameter, std::unique_ptr<request> &req, std::unique_ptr<reply> &rep, std::string message);
  static uint64_t validate_id(const std::unique_ptr<request> &req, std::unique_ptr<reply> &rep);
  static uint64_t validate_id2(const std::unique_ptr<request> &req, std::unique_ptr<reply> &rep);
  static uint64_t validate_limit(std::unique_ptr<request> &req, std::unique_ptr<reply> &rep);
  static uint64_t validate_offset(std::unique_ptr<request> &req, std::unique_ptr<reply> &rep);
  static void convert_property_to_json(std::unique_ptr<reply> &rep, const std::any &property);
};


#endif//TRITON_SERVER_H
