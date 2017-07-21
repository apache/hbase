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

#include <glog/logging.h>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <sasl/saslutil.h>

#include <memory>
#include <string>
#include <vector>

#include "connection/sasl-util.h"
#include "connection/service.h"
#include "security/user.h"
#include "serde/rpc-serde.h"

namespace hbase {

/**
 * Class to perform SASL handshake with server (currently works with regionserver principals only)
 * It is inserted between EventBaseHandler and LengthFieldBasedFrameDecoder in the pipeline
 * SaslHandler would intercept writes to server by buffering the IOBuf's and start the handshake
 * process
 *   (via sasl_client_XX calls provided by Cyrus)
 * After handshake is complete, SaslHandler would send the buffered IOBuf's to server and
 *   act as pass-thru from then on
 */
class SaslHandler
    : public wangle::HandlerAdapter<folly::IOBufQueue&, std::unique_ptr<folly::IOBuf>> {
 public:
  explicit SaslHandler(std::string user_name, std::shared_ptr<Configuration> conf);
  SaslHandler(const SaslHandler& hdlr);
  ~SaslHandler();

  // from HandlerAdapter
  void read(Context* ctx, folly::IOBufQueue& buf) override;
  folly::Future<folly::Unit> write(Context* ctx, std::unique_ptr<folly::IOBuf> buf) override;
  void transportActive(Context* ctx) override;

 private:
  // used by Cyrus
  sasl_conn_t* sconn_ = nullptr;
  std::string user_name_;
  std::string service_name_;
  std::string host_name_;
  bool secure_;
  std::atomic_flag sasl_connection_setup_started_;
  std::atomic<bool> sasl_connection_setup_in_progress_{true};
  // vector of folly::IOBuf which buffers client writes before handshake is complete
  std::vector<std::unique_ptr<folly::IOBuf>> iobuf_;
  SaslUtil sasl_util_;

  // writes the output returned by sasl_client_XX to server
  folly::Future<folly::Unit> WriteSaslOutput(Context* ctx, const char* out, unsigned int outlen);
  folly::Future<folly::Unit> SaslInit(Context* ctx);
  void FinishAuth(Context* ctx, folly::IOBufQueue* bufQueue);
  void ContinueSaslNegotiation(Context* ctx, folly::IOBufQueue* buf);
  std::string ParseServiceName(std::shared_ptr<Configuration> conf, bool secure);
};
}  // namespace hbase
