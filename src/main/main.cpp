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

#include "../lib/seastar/stop_signal.hh"
#include "server/Degrees.h"
#include "server/Lua.h"
#include "server/NodeProperties.h"
#include "server/Nodes.h"
#include "server/RelationshipProperties.h"
#include "server/Relationships.h"
#include "server/Neighbors.h"
#include <Graph.h>
#include <chrono>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/reactor.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <sol.hpp>

namespace bpo = boost::program_options;
using namespace std::chrono;
using namespace triton;
using namespace seastar;
using namespace httpd;

int main(int argc, char** argv) {
  // Our Graph
  Graph graph("triton");
  // Prometheus Server Metrics
  httpd::http_server_control prometheus_server;
  prometheus::config pctx;
  seastar::app_template app;

  //Options
  app.add_options()("address", bpo::value<sstring>()->default_value("0.0.0.0"), "HTTP Server address");
  app.add_options()("port", bpo::value<uint16_t>()->default_value(10000), "HTTP Server port");
  app.add_options()("prometheus_port", bpo::value<uint16_t>()->default_value(9180), "Prometheus port. Set to zero in order to disable.");
  app.add_options()("prometheus_address", bpo::value<sstring>()->default_value("0.0.0.0"), "Prometheus address");
  app.add_options()("prometheus_prefix", bpo::value<sstring>()->default_value("triton_httpd"), "Prometheus metrics prefix");

  return app.run(argc, argv, [&] {
    std::cout << "Running on " << seastar::smp::count << " cores." << '\n';
    seastar::engine().at_exit([&] { return graph.stop(); });

    return seastar::async([&] {
           seastar_apps_lib::stop_signal stop_signal;
           auto&& config = app.configuration();
           httpd::http_server_control prometheus_server;

           // Start Prometheus
           uint16_t pport = config["prometheus_port"].as<uint16_t>();
           if (pport) {
             prometheus::config pctx;
             net::inet_address prom_addr(config["prometheus_address"].as<sstring>());

             pctx.metric_help = "seastar::httpd server statistics";
             pctx.prefix = config["prometheus_prefix"].as<sstring>();

             std::cout << "starting prometheus API server" << std::endl;
             prometheus_server.start("prometheus").get();

             prometheus::start(prometheus_server, pctx).get();

             prometheus_server.listen(socket_address{prom_addr, pport}).handle_exception([prom_addr, pport] (auto ep) {
                    std::cerr << seastar::format("Could not start Prometheus API server on {}:{}: {}\n", prom_addr, pport, ep);
                    return make_exception_future<>(ep);
             }).get();
           }

           // Initialize Graph
           graph.start().get();

           // Initialize Routes?
           Nodes nodes = Nodes(graph);
           Relationships relationships = Relationships(graph);
           Degrees degrees = Degrees(graph);
           Neighbors neighbors = Neighbors(graph);
           NodeProperties nodeProperties = NodeProperties(graph);
           RelationshipProperties relationshipProperties = RelationshipProperties(graph);
           Lua lua = Lua(graph);

           // Start Server
           net::inet_address addr(config["address"].as<sstring>());
           uint16_t port = config["port"].as<uint16_t>();
           auto server = new http_server_control();
           auto rb = make_shared<api_registry_builder>("apps/httpd/");

           server->start().get();
           server->set_routes([&relationshipProperties](routes& r) { relationshipProperties.set_routes(r);}).get();
           server->set_routes([&nodeProperties](routes& r) { nodeProperties.set_routes(r);}).get();
           server->set_routes([&degrees](routes& r) { degrees.set_routes(r);}).get();
           server->set_routes([&neighbors](routes& r) {neighbors.set_routes(r);}).get();
           server->set_routes([&nodes](routes& r) { nodes.set_routes(r);}).get();
           server->set_routes([&relationships](routes& r) { relationships.set_routes(r);}).get();
           server->set_routes([&lua](routes& r) { lua.set_routes(r);}).get();
           server->set_routes([rb](routes& r){rb->set_api_doc(r);}).get();
           server->listen(socket_address{addr, port}).get();

           std::cout << "Triton HTTP server listening on " << addr << ":" << port << " ...\n";
           engine().at_exit([&prometheus_server, server, pport] {
                  return [pport, &prometheus_server] {
                         if (pport > 0) {
                           std::cout << "Stopping Prometheus server" << std::endl;
                           return prometheus_server.stop();
                         }
                         return make_ready_future<>();
                  }().finally([server] {
                         std::cout << "Stopping HTTP server" << std::endl;
                         return server->stop();
                  });
           });

           stop_signal.wait().get();

    });
  });
}


