#ifndef TRITON_STOP_SIGNAL_HH
#define TRITON_STOP_SIGNAL_HH

/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2020 Cloudius Systems, Ltd.
 */

#include <seastar/core/sharded.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/condition-variable.hh>

namespace seastar_apps_lib {

  class stop_signal {
    bool _caught = false;
    seastar::condition_variable _cond;
  private:
    void signaled() {
      if (_caught) {
        return;
      }
      _caught = true;
      _cond.broadcast();
    }
  public:
    stop_signal() {
      seastar::engine().handle_signal(SIGINT, [this] { signaled(); });
      seastar::engine().handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
      // There's no way to unregister a handler yet, so register a no-op handler instead.
      seastar::engine().handle_signal(SIGINT, [] {});
      seastar::engine().handle_signal(SIGTERM, [] {});
    }
    seastar::future<> wait() {
      return _cond.wait([this] { return _caught; });
    }
    bool stopping() const {
      return _caught;
    }
  };
} // namespace seastar_apps_lib
#endif//TRITON_STOP_SIGNAL_HH
