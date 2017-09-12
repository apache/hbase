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

#include "hbase/connection/client-handler.h"

#include <folly/ExceptionWrapper.h>
#include <folly/Likely.h>
#include <folly/io/async/AsyncSocketException.h>
#include <glog/logging.h>
#include <string>

#include "hbase/connection/request.h"
#include "hbase/connection/response.h"
#include "hbase/if/Client.pb.h"
#include "hbase/if/RPC.pb.h"

using google::protobuf::Message;

namespace hbase {

ClientHandler::ClientHandler(std::string user_name, std::shared_ptr<Codec> codec,
                             std::shared_ptr<Configuration> conf, const std::string &server)
    : user_name_(user_name),
      serde_(codec),
      conf_(conf),
      server_(server),
      once_flag_(std::make_unique<std::once_flag>()),
      resp_msgs_(
          std::make_unique<concurrent_map<uint32_t, std::shared_ptr<google::protobuf::Message>>>(
              5000)) {}

void ClientHandler::read(Context *ctx, std::unique_ptr<folly::IOBuf> buf) {
  if (LIKELY(buf != nullptr)) {
    buf->coalesce();
    auto received = std::make_unique<Response>();
    pb::ResponseHeader header;

    int used_bytes = serde_.ParseDelimited(buf.get(), &header);
    VLOG(3) << "Read RPC ResponseHeader size=" << used_bytes << " call_id=" << header.call_id()
            << " has_exception=" << header.has_exception() << ", server: " << server_;

    auto resp_msg = resp_msgs_->find_and_erase(header.call_id());

    // set the call_id.
    // This will be used to by the dispatcher to match up
    // the promise with the response.
    received->set_call_id(header.call_id());

    // If there was an exception then there's no
    // data left on the wire.
    if (header.has_exception() == false) {
      buf->trimStart(used_bytes);

      int cell_block_length = 0;
      used_bytes = serde_.ParseDelimited(buf.get(), resp_msg.get());
      if (header.has_cell_block_meta() && header.cell_block_meta().has_length()) {
        cell_block_length = header.cell_block_meta().length();
      }

      VLOG(3) << "Read RPCResponse, buf length:" << buf->length()
              << ", header PB length:" << used_bytes << ", cell_block length:" << cell_block_length
              << ", server: " << server_;

      // Make sure that bytes were parsed.
      CHECK((used_bytes + cell_block_length) == buf->length());

      if (cell_block_length > 0) {
        auto cell_scanner = serde_.CreateCellScanner(std::move(buf), used_bytes, cell_block_length);
        received->set_cell_scanner(std::shared_ptr<CellScanner>{cell_scanner.release()});
      }

      received->set_resp_msg(resp_msg);
    } else {
      hbase::pb::ExceptionResponse exceptionResponse = header.exception();

      std::string what;
      std::string exception_class_name = exceptionResponse.has_exception_class_name()
                                             ? exceptionResponse.exception_class_name()
                                             : "";
      std::string stack_trace =
          exceptionResponse.has_stack_trace() ? exceptionResponse.stack_trace() : "";
      what.append(stack_trace);

      auto remote_exception = std::make_unique<RemoteException>(what);
      remote_exception->set_exception_class_name(exception_class_name)
          ->set_stack_trace(stack_trace)
          ->set_hostname(exceptionResponse.has_hostname() ? exceptionResponse.hostname() : "")
          ->set_port(exceptionResponse.has_port() ? exceptionResponse.port() : 0);
      if (exceptionResponse.has_do_not_retry()) {
        remote_exception->set_do_not_retry(exceptionResponse.do_not_retry());
      }

      VLOG(3) << "Exception RPC ResponseHeader, call_id=" << header.call_id()
              << " exception.what=" << remote_exception->what()
              << ", do_not_retry=" << remote_exception->do_not_retry() << ", server: " << server_;
      received->set_exception(folly::exception_wrapper{*remote_exception});
    }
    ctx->fireRead(std::move(received));
  }
}

folly::Future<folly::Unit> ClientHandler::write(Context *ctx, std::unique_ptr<Request> r) {
  /* for RPC test, there's no need to send connection header */
  if (!conf_->GetBool(RpcSerde::HBASE_CLIENT_RPC_TEST_MODE,
                      RpcSerde::DEFAULT_HBASE_CLIENT_RPC_TEST_MODE)) {
    // We need to send the header once.
    // So use call_once to make sure that only one thread wins this.
    std::call_once((*once_flag_), [ctx, this]() {
      VLOG(3) << "Writing RPC Header to server: " << server_;
      auto header = serde_.Header(user_name_);
      ctx->fireWrite(std::move(header));
    });
  }

  VLOG(3) << "Writing RPC Request:" << r->DebugString() << ", server: " << server_;

  // Now store the call id to response.
  resp_msgs_->insert(std::make_pair(r->call_id(), r->resp_msg()));

  try {
    // Send the data down the pipeline.
    return ctx->fireWrite(serde_.Request(r->call_id(), r->method(), r->req_msg().get()));
  } catch (const folly::AsyncSocketException &e) {
    /* clear protobuf::Message to avoid overflow. */
    resp_msgs_->erase(r->call_id());
    throw e;
  }
}
}  // namespace hbase
