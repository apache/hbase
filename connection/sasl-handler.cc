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
#include "connection/sasl-handler.h"

#include <glog/logging.h>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <sasl/saslutil.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <wangle/channel/Handler.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "connection/service.h"
#include "security/user.h"
using hbase::security::User;

using std::chrono::nanoseconds;
using namespace folly;
using namespace wangle;
using namespace hbase;

SaslHandler::SaslHandler(std::string user_name, std::shared_ptr<Configuration> conf)
    : user_name_(user_name) {
  host_name_.clear();
  secure_ = User::IsSecurityEnabled(*conf);
  service_name_ = SaslUtil::ParseServiceName(conf, secure_);
  sasl_connection_setup_started_.clear();
  sasl_connection_setup_in_progress_.store(true);
}

SaslHandler::SaslHandler(const SaslHandler &hdlr) {
  user_name_ = hdlr.user_name_;
  service_name_ = hdlr.service_name_;
  secure_ = hdlr.secure_;
  host_name_ = hdlr.host_name_;
  // copy-constructor sets the flags below to their initial state as opposed to getting them
  // from the object this class is constructed from. That way, this instance is ready to do
  // sasl stuff without issues, right from the SaslInit. Sharing a sasl session is not useful
  // between two handler instances.
  sasl_connection_setup_started_.clear();
  sasl_connection_setup_in_progress_.store(true);
  sconn_ = nullptr;
}

SaslHandler::~SaslHandler() {
  if (nullptr != sconn_) {
    sasl_dispose(&sconn_);
  }
  sconn_ = nullptr;
}

void SaslHandler::transportActive(Context *ctx) {
  // assign hostname; needed for the sasl handshake if secure
  folly::SocketAddress address;
  ctx->getTransport()->getPeerAddress(&address);
  host_name_ = address.getHostStr();

  // now init the sasl library; this is once per process
  if (secure_) {
    sasl_util_.InitializeSaslLib();
  }
  // write the preamble to kick off the RPC handshake
  VLOG(3) << "Writing RPC connection Preamble to server: " << host_name_;
  auto preamble = RpcSerde::Preamble(secure_);
  ctx->fireWrite(std::move(preamble));
}

void SaslHandler::read(Context *ctx, folly::IOBufQueue &buf) {
  // if security is not on, or in case of security-on, if secure connection setup not in progress,
  // pass it up without touching
  if (!secure_ || !sasl_connection_setup_in_progress_.load()) {
    ctx->fireRead(buf);
  } else {
    // message is for this handler; process it appropriately
    ContinueSaslNegotiation(ctx, &buf);
  }
}

folly::Future<folly::Unit> SaslHandler::write(Context *ctx, std::unique_ptr<folly::IOBuf> buf) {
  // if security is on, and if secure connection setup in progress,
  // this message is for this handler to process and respond
  if (secure_ && sasl_connection_setup_in_progress_.load()) {
    // store IOBuf which is to be sent to server after SASL handshake
    iobuf_.push_back(std::move(buf));
    if (!sasl_connection_setup_started_.test_and_set()) {
      // for the first incoming RPC from the higher layer, trigger sasl initialization
      return SaslInit(ctx);
    } else {
      // for the subsequent incoming RPCs from the higher layer, just return empty future
      folly::Promise<folly::Unit> p_;
      return p_.getFuture();
    }
  }
  // pass the bytes recieved down without touching it
  return ctx->fireWrite(std::move(buf));
}

folly::Future<folly::Unit> SaslHandler::WriteSaslOutput(Context *ctx, const char *out,
                                                        unsigned int outlen) {
  int buffer_size = outlen + 4;
  auto iob = IOBuf::create(buffer_size);
  iob->append(buffer_size);
  // Create the array output stream.
  google::protobuf::io::ArrayOutputStream aos{iob->writableData(), buffer_size};
  std::unique_ptr<google::protobuf::io::CodedOutputStream> coded_output =
      std::make_unique<google::protobuf::io::CodedOutputStream>(&aos);
  uint32_t total_size = outlen;
  total_size = ntohl(total_size);
  coded_output->WriteRaw(&total_size, 4);
  coded_output->WriteRaw(out, outlen);
  return ctx->fireWrite(std::move(iob));
}

void SaslHandler::FinishAuth(Context *ctx, folly::IOBufQueue *bufQueue) {
  std::unique_ptr<folly::IOBuf> iob;
  if (!bufQueue->empty()) {
    iob = bufQueue->pop_front();
    throw std::runtime_error("Error in the final step of handshake " +
                             std::string(reinterpret_cast<const char *>(iob->data())));
  } else {
    sasl_connection_setup_in_progress_.store(false);
    // write what we buffered
    for (size_t i = 0; i < iobuf_.size(); i++) {
      iob = std::move(iobuf_.at(i));
      ctx->fireWrite(std::move(iob));
    }
  }
}

folly::Future<folly::Unit> SaslHandler::SaslInit(Context *ctx) {
  int rc;
  const char *mechusing, *mechlist = "GSSAPI";
  const char *out;
  unsigned int outlen;

  rc = sasl_client_new(service_name_.c_str(), /* The service we are using*/
                       host_name_.c_str(), NULL,
                       NULL, /* Local and remote IP address strings
                                   (NULL disables mechanisms which require this info)*/
                       NULL, /*connection-specific callbacks*/
                       0 /*security flags*/, &sconn_);
  if (rc != SASL_OK) {
    LOG(FATAL) << "Cannot create client (" << rc << ") ";
    throw std::runtime_error("Cannot create client");
  }
  int curr_rc;
  do {
    curr_rc = sasl_client_start(sconn_,   /* the same context from above */
                                mechlist, /* the list of mechanisms from the server */
                                NULL,     /* filled in if an interaction is needed */
                                &out,     /* filled in on success */
                                &outlen,  /* filled in on success */
                                &mechusing);
  } while (curr_rc == SASL_INTERACT); /* the mechanism may ask us to fill
     in things many times. result is SASL_CONTINUE on success */
  if (curr_rc != SASL_CONTINUE) {
    throw std::runtime_error("Cannot start client (" + std::to_string(curr_rc) + ")");
  }
  folly::Future<folly::Unit> fut = WriteSaslOutput(ctx, out, outlen);
  return fut;
}

void SaslHandler::ContinueSaslNegotiation(Context *ctx, folly::IOBufQueue *bufQueue) {
  const char *out;
  unsigned int outlen;

  int bytes_sent = 0;
  int bytes_received = 0;

  std::unique_ptr<folly::IOBuf> iob = bufQueue->pop_front();
  bytes_received = iob->length();
  if (bytes_received == 0) {
    throw std::runtime_error("Error in sasl handshake");
  }
  folly::io::RWPrivateCursor c(iob.get());
  std::uint32_t status = c.readBE<std::uint32_t>();
  std::uint32_t sz = c.readBE<std::uint32_t>();

  if (status != 0 /*Status 0 is success*/) {
    // Assumption here is that the response from server is not more than 8 * 1024
    throw std::runtime_error("Error in sasl handshake " +
                             std::string(reinterpret_cast<char *>(c.writableData())));
  }
  out = nullptr;
  outlen = 0;

  int curr_rc =
      sasl_client_step(sconn_,                                     /* our context */
                       reinterpret_cast<char *>(c.writableData()), /* the data from the server */
                       sz,                                         /* its length */
                       NULL,     /* this should be unallocated and NULL */
                       &out,     /* filled in on success */
                       &outlen); /* filled in on success */

  if (curr_rc == SASL_OK || curr_rc == SASL_CONTINUE) {
    WriteSaslOutput(ctx, out, outlen);
  }
  if (curr_rc == SASL_OK) {
    FinishAuth(ctx, bufQueue);
  }
}
