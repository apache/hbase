/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#pragma once

#include "if/HBase.pb.h"
#include "security/user.h"

#include <memory>
#include <utility>
#include <boost/functional/hash.hpp>

using hbase::pb::ServerName;
using hbase::security::User;

namespace hbase {
class ConnectionId {
 public:
  ConnectionId(const std::string &host, uint16_t port)
      : ConnectionId(host, port, User::defaultUser(), "") {}

  ConnectionId(const std::string &host, uint16_t port,
               std::shared_ptr<User> user)
      : ConnectionId(host, port, user, "") {}

  ConnectionId(const std::string &host, uint16_t port,
               std::shared_ptr<User> user, const std::string &service_name)
      : user_(user), service_name_(service_name), host_(host), port_(port) {}

  virtual ~ConnectionId() = default;

  std::shared_ptr<User> user() const { return user_; }
  std::string service_name() const { return service_name_; }
  std::string host() { return host_; }
  uint16_t port() { return port_; }

 private:
  std::shared_ptr<User> user_;
  std::string service_name_;
  std::string host_;
  uint16_t port_;
};

/* Equals function for ConnectionId */
struct ConnectionIdEquals {
  /** equals */
  bool operator()(const std::shared_ptr<ConnectionId> &lhs,
                  const std::shared_ptr<ConnectionId> &rhs) const {
    return userEquals(lhs->user(), rhs->user()) && lhs->host() == rhs->host() &&
           lhs->port() == rhs->port();
  }

 private:
  bool userEquals(const std::shared_ptr<User> &lhs,
                  const std::shared_ptr<User> &rhs) const {
    return lhs == nullptr ? rhs == nullptr
                          : (rhs == nullptr ? false : lhs->user_name() ==
                                                          rhs->user_name());
  }
};

/** Hash for ConnectionId. */
struct ConnectionIdHash {
  /** hash */
  std::size_t operator()(const std::shared_ptr<ConnectionId> &ci) const {
    std::size_t h = 0;
    boost::hash_combine(h, ci->user() == nullptr ? 0 : ci->user()->user_name());
    boost::hash_combine(h, ci->host());
    boost::hash_combine(h, ci->port());
    return h;
  }
};
}  // namespace hbase
