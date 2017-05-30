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

#include "core/async-batch-rpc-retrying-caller.h"
#include <glog/logging.h>
#include <limits>

using folly::Future;
using folly::Promise;
using folly::Try;
using hbase::pb::ServerName;
using hbase::pb::TableName;
using hbase::security::User;
using std::chrono::nanoseconds;
using std::chrono::milliseconds;

namespace hbase {

AsyncBatchRpcRetryingCaller::AsyncBatchRpcRetryingCaller(
    std::shared_ptr<AsyncConnection> conn, std::shared_ptr<folly::HHWheelTimer> retry_timer,
    std::shared_ptr<TableName> table_name, const std::vector<hbase::Get> &actions,
    nanoseconds pause_ns, int32_t max_attempts, nanoseconds operation_timeout_ns,
    nanoseconds rpc_timeout_ns, int32_t start_log_errors_count)
    : conn_(conn),
      retry_timer_(retry_timer),
      table_name_(table_name),
      pause_ns_(pause_ns),
      operation_timeout_ns_(operation_timeout_ns),
      rpc_timeout_ns_(rpc_timeout_ns),
      start_log_errors_count_(start_log_errors_count) {
  CHECK(conn_ != nullptr);
  CHECK(retry_timer_ != nullptr);
  location_cache_ = conn_->region_locator();
  rpc_client_ = conn_->rpc_client();
  cpu_pool_ = conn_->cpu_executor();
  CHECK(location_cache_ != nullptr);
  CHECK(rpc_client_ != nullptr);
  CHECK(cpu_pool_ != nullptr);

  max_attempts_ = ConnectionUtils::Retries2Attempts(max_attempts);
  uint32_t index = 0;
  for (auto row : actions) {
    actions_.push_back(std::make_shared<Action>(std::make_shared<hbase::Get>(row), index));
    Promise<std::shared_ptr<Result>> prom{};
    action2promises_.insert(
        std::pair<uint64_t, Promise<std::shared_ptr<Result>>>(index, std::move(prom)));
    action2futures_.push_back(action2promises_[index++].getFuture());
  }
}

AsyncBatchRpcRetryingCaller::~AsyncBatchRpcRetryingCaller() {}

Future<std::vector<Try<std::shared_ptr<Result>>>> AsyncBatchRpcRetryingCaller::Call() {
  GroupAndSend(actions_, 1);
  return collectAll(action2futures_);
}

int64_t AsyncBatchRpcRetryingCaller::RemainingTimeNs() {
  return operation_timeout_ns_.count() - (TimeUtil::GetNowNanos() - start_ns_);
}

void AsyncBatchRpcRetryingCaller::LogException(int32_t tries,
                                               std::shared_ptr<RegionRequest> region_request,
                                               std::shared_ptr<std::exception> &error,
                                               std::shared_ptr<ServerName> server_name) {
  if (tries > start_log_errors_count_) {
    std::string regions;
    regions += region_request->region_location()->region_name() + ", ";
    LOG(WARNING) << "Process batch for " << regions << " in " << table_name_->namespace_() << ":"
                 << table_name_->qualifier() << " from " << server_name->host_name()
                 << " failed, tries=" << tries << ":- " << error->what();
  }
}

void AsyncBatchRpcRetryingCaller::LogException(
    int32_t tries, std::vector<std::shared_ptr<RegionRequest>> &region_requests,
    std::shared_ptr<std::exception> &error, std::shared_ptr<ServerName> server_name) {
  if (tries > start_log_errors_count_) {
    std::string regions;
    for (const auto region_request : region_requests) {
      regions += region_request->region_location()->region_name() + ", ";
    }
    LOG(WARNING) << "Process batch for " << regions << " in " << table_name_->namespace_() << ":"
                 << table_name_->qualifier() << " from " << server_name->host_name()
                 << " failed, tries=" << tries << error->what();
  }
}

const std::string AsyncBatchRpcRetryingCaller::GetExtraContextForError(
    std::shared_ptr<ServerName> server_name) {
  return server_name ? server_name->ShortDebugString() : "";
}

// TODO HBASE-17800 pass folly ew instead of std::exception
void AsyncBatchRpcRetryingCaller::AddError(const std::shared_ptr<Action> &action,
                                           std::shared_ptr<std::exception> error,
                                           std::shared_ptr<ServerName> server_name) {
  folly::exception_wrapper ew;
  ThrowableWithExtraContext twec(ew, TimeUtil::GetNowNanos(), GetExtraContextForError(server_name));
  AddAction2Error(action->original_index(), twec);
}

void AsyncBatchRpcRetryingCaller::AddError(const std::vector<std::shared_ptr<Action>> &actions,
                                           std::shared_ptr<std::exception> error,
                                           std::shared_ptr<ServerName> server_name) {
  for (const auto action : actions) {
    AddError(action, error, server_name);
  }
}

// TODO HBASE-17800 pass folly ew instead of std::exception
void AsyncBatchRpcRetryingCaller::FailOne(const std::shared_ptr<Action> &action, int32_t tries,
                                          std::shared_ptr<std::exception> error,
                                          int64_t current_time, const std::string extras) {
  auto action_index = action->original_index();
  auto itr = action2promises_.find(action_index);
  if (itr != action2promises_.end()) {
    if (itr->second.isFulfilled()) {
      return;
    }
  }
  folly::exception_wrapper ew;
  ThrowableWithExtraContext twec(ew, current_time, extras);
  AddAction2Error(action_index, twec);
  action2promises_[action_index].setException(
      RetriesExhaustedException(tries - 1, action2errors_[action_index]));
}

void AsyncBatchRpcRetryingCaller::FailAll(const std::vector<std::shared_ptr<Action>> &actions,
                                          int32_t tries, std::shared_ptr<std::exception> error,
                                          std::shared_ptr<ServerName> server_name) {
  for (const auto action : actions) {
    FailOne(action, tries, error, TimeUtil::GetNowNanos(), GetExtraContextForError(server_name));
  }
}

void AsyncBatchRpcRetryingCaller::FailAll(const std::vector<std::shared_ptr<Action>> &actions,
                                          int32_t tries) {
  for (const auto action : actions) {
    auto action_index = action->original_index();
    auto itr = action2promises_.find(action_index);
    if (itr->second.isFulfilled()) {
      return;
    }
    action2promises_[action_index].setException(
        RetriesExhaustedException(tries - 1, action2errors_[action_index]));
  }
}

void AsyncBatchRpcRetryingCaller::AddAction2Error(uint64_t action_index,
                                                  const ThrowableWithExtraContext &twec) {
  auto erritr = action2errors_.find(action_index);
  if (erritr != action2errors_.end()) {
    erritr->second->push_back(twec);
  } else {
    action2errors_[action_index] = std::make_shared<std::vector<ThrowableWithExtraContext>>();
    action2errors_[action_index]->push_back(twec);
  }
  return;
}

void AsyncBatchRpcRetryingCaller::OnError(const ActionsByRegion &actions_by_region, int32_t tries,
                                          std::shared_ptr<std::exception> exc,
                                          std::shared_ptr<ServerName> server_name) {
  std::vector<std::shared_ptr<Action>> copied_actions;
  std::vector<std::shared_ptr<RegionRequest>> region_requests;
  for (const auto &action_by_region : actions_by_region) {
    region_requests.push_back(action_by_region.second);
    // Concurrent
    for (const auto &action : action_by_region.second->actions()) {
      copied_actions.push_back(action);
    }
  }
  // TODO HBASE-17800 for exc check with DoNotRetryIOException
  LogException(tries, region_requests, exc, server_name);
  if (tries >= max_attempts_) {
    FailAll(copied_actions, tries, exc, server_name);
    return;
  }
  AddError(copied_actions, exc, server_name);
  TryResubmit(copied_actions, tries);
}

void AsyncBatchRpcRetryingCaller::TryResubmit(std::vector<std::shared_ptr<Action>> actions,
                                              int32_t tries) {
  int64_t delay_ns;
  if (operation_timeout_ns_.count() > 0) {
    int64_t max_delay_ns = RemainingTimeNs() - ConnectionUtils::kSleepDeltaNs;
    if (max_delay_ns <= 0) {
      VLOG(8) << "Fail All from onError";
      FailAll(actions, tries);
      return;
    }
    delay_ns = std::min(max_delay_ns, ConnectionUtils::GetPauseTime(pause_ns_.count(), tries - 1));
  } else {
    delay_ns = ConnectionUtils::GetPauseTime(pause_ns_.count(), tries - 1);
  }
  // TODO This gives segfault @ present, when retried
  //    retry_timer_->scheduleTimeoutFn([&]() { GroupAndSend(actions, tries + 1); },
  //                                  milliseconds(TimeUtil::ToMillis(delay_ns)));
}

Future<std::vector<Try<std::shared_ptr<RegionLocation>>>>
AsyncBatchRpcRetryingCaller::GetRegionLocations(const std::vector<std::shared_ptr<Action>> &actions,
                                                int64_t locate_timeout_ns) {
  auto locs = std::vector<Future<std::shared_ptr<RegionLocation>>>{};
  for (auto const &action : actions) {
    locs.push_back(location_cache_->LocateRegion(*table_name_, action->action()->row(),
                                                 RegionLocateType::kCurrent, locate_timeout_ns));
  }

  return collectAll(locs);
}

void AsyncBatchRpcRetryingCaller::GroupAndSend(const std::vector<std::shared_ptr<Action>> &actions,
                                               int32_t tries) {
  int64_t locate_timeout_ns;
  if (operation_timeout_ns_.count() > 0) {
    locate_timeout_ns = RemainingTimeNs();
    if (locate_timeout_ns <= 0) {
      FailAll(actions_, tries);
      return;
    }
  } else {
    locate_timeout_ns = -1L;
  }

  GetRegionLocations(actions, locate_timeout_ns)
      .then([&](std::vector<Try<std::shared_ptr<RegionLocation>>> &loc) {
        std::lock_guard<std::mutex> lock(multi_mutex_);
        ActionsByServer actions_by_server;
        std::vector<std::shared_ptr<Action>> locate_failed;

        for (uint64_t i = 0; i < loc.size(); ++i) {
          auto action = actions[i];
          if (loc[i].hasValue()) {
            auto region_loc = loc[i].value();
            // Add it to actions_by_server;
            // Concurrent
            auto search =
                actions_by_server.find(std::make_shared<ServerName>(region_loc->server_name()));
            if (search != actions_by_server.end()) {
              search->second->AddActionsByRegion(region_loc, action);
            } else {
              // Create new key
              auto server_request = std::make_shared<ServerRequest>(region_loc);
              server_request->AddActionsByRegion(region_loc, action);
              auto server_name = std::make_shared<ServerName>(region_loc->server_name());
              actions_by_server[server_name] = server_request;
            }
            locate_failed.push_back(action);
            VLOG(8) << "row [" << action->action()->row() << "] of table["
                    << table_name_->namespace_() << ":" << table_name_->qualifier()
                    << " found in region [" << region_loc->region_name() << "]; host["
                    << region_loc->server_name().host_name() << "]; port["
                    << region_loc->server_name().port() << "];";
          } else if (loc[i].hasException()) {
            VLOG(8) << "Exception occured while locating region:- "
                    << loc[i].exception().getCopied()->what() << " for action index " << i;
            // TODO Feedback needed, Java API only identifies DoNotRetryIOException
            // We might receive runtime error from location-cache.cc too, we are treating both same
            if (loc[i].exception().is_compatible_with<std::runtime_error>()) {
              std::string extra = "";
              FailOne(action, tries, nullptr, TimeUtil::GetNowNanos(),
                      loc[i].exception().what().toStdString());
              return;
            }
            // TODO HBASE-17800 for exc check with DoNotRetryIOException
            /*
             else if (loc[i].exception().is_compatible_with<hbase::DoNotRetryIOException>()) {
             int64_t current_time = 0;
             std::string extra = "";
             FailOne(action, tries, nullptr, TimeUtil::GetNowNanos(),
             loc[i].exception().what().toStdString());
             return;
             }*/
            AddError(action, std::make_shared<std::exception>(*loc[i].exception().getCopied()),
                     nullptr);
            locate_failed.push_back(action);
          }
        }

        if (!actions_by_server.empty()) {
          Send(actions_by_server, tries);
        }

        if (!locate_failed.empty()) {
          TryResubmit(locate_failed, tries);
        }
      })
      .onError([&](const folly::exception_wrapper &ew) {
        std::lock_guard<std::mutex> lock(multi_mutex_);
        auto exc = ew.getCopied();
        VLOG(8) << "GetRegionLocations() exception: " << ew.what().toStdString();
      });
  return;
}

Future<std::vector<Try<std::unique_ptr<Response>>>> AsyncBatchRpcRetryingCaller::GetMultiResponse(
    const ActionsByServer &actions_by_server) {
  // Concurrent.
  auto multi_calls = std::vector<Future<std::unique_ptr<hbase::Response>>>{};
  auto user = User::defaultUser();
  for (const auto &action_by_server : actions_by_server) {
    std::unique_ptr<Request> multi_req =
        RequestConverter::ToMultiRequest(action_by_server.second->actions_by_region());
    auto host = action_by_server.first->host_name();
    int port = action_by_server.first->port();
    multi_calls.push_back(
        rpc_client_->AsyncCall(host, port, std::move(multi_req), user, "ClientService"));
  }
  return collectAll(multi_calls);
}

void AsyncBatchRpcRetryingCaller::Send(ActionsByServer &actions_by_server, int32_t tries) {
  int64_t remaining_ns;
  if (operation_timeout_ns_.count() > 0) {
    remaining_ns = RemainingTimeNs();
    if (remaining_ns <= 0) {
      std::vector<std::shared_ptr<Action>> failed_actions;
      for (const auto &action_by_server : actions_by_server) {
        // Concurrent
        for (auto &value : action_by_server.second->actions_by_region()) {
          // Concurrent
          for (const auto &failed_action : value.second->actions()) {
            failed_actions.push_back(failed_action);
          }
        }
      }
      FailAll(failed_actions, tries);
      return;
    }
  } else {
    remaining_ns = std::numeric_limits<int64_t>::max();
  }

  std::vector<std::shared_ptr<Request>> multi_reqv;
  for (const auto &action_by_server : actions_by_server)
    multi_reqv.push_back(
        std::move(RequestConverter::ToMultiRequest(action_by_server.second->actions_by_region())));

  GetMultiResponse(actions_by_server)
      .then([=](const std::vector<Try<std::unique_ptr<hbase::Response>>> &completed_responses) {
        std::lock_guard<std::mutex> lock(multi_mutex_);
        for (uint64_t num = 0; num < completed_responses.size(); ++num) {
          if (completed_responses[num].hasValue()) {
            auto multi_response =
                ResponseConverter::GetResults(multi_reqv[num], *completed_responses[num].value());
            for (const auto &action_by_server : actions_by_server) {
              OnComplete(action_by_server.second->actions_by_region(), tries,
                         action_by_server.first, std::move(multi_response));
            }
          } else if (completed_responses[num].hasException()) {
            VLOG(8) << "Received exception: "
                    << completed_responses[num].exception().getCopied()->what()
                    << " from server for action index " << num;
            // TODO: we should call OnError here as well.
          }
        }
      })
      .onError([=](const folly::exception_wrapper &ew) {
        auto exc = ew.getCopied();
        VLOG(8) << "GetMultiResponse() exception: " << ew.what().toStdString();
        std::lock_guard<std::mutex> lock(multi_mutex_);
        for (const auto &action_by_server : actions_by_server) {
          OnError(action_by_server.second->actions_by_region(), tries,
                  std::make_shared<std::exception>(*exc), action_by_server.first);
        }
      });
  return;
}

void AsyncBatchRpcRetryingCaller::OnComplete(
    const ActionsByRegion &actions_by_region, int32_t tries,
    const std::shared_ptr<ServerName> server_name,
    const std::unique_ptr<hbase::MultiResponse> multi_response) {
  std::vector<std::shared_ptr<Action>> failed_actions;
  for (const auto &action_by_region : actions_by_region) {
    auto region_result_itr = multi_response->RegionResults().find(action_by_region.first);
    if (region_result_itr == multi_response->RegionResults().end()) {
      VLOG(8) << "Region " << action_by_region.first << " not found in MultiResults.";
      // TODO Feedback needed Should we throw from here or continue for next action_by_region ?
      // Throwing at present as this looks like an inconsistency
      // Concurrent
      auto exc = std::make_shared<std::runtime_error>("Invalid search for region " +
                                                      action_by_region.first + " in multi results");
      FailAll(action_by_region.second->actions(), tries, exc, server_name);
      return;
      // std::runtime_error(
      //  "Invalid search for region " + action_by_region.first + " in multi results");
    }
    if (region_result_itr != multi_response->RegionResults().end()) {
      // Concurrent
      for (const auto &action : action_by_region.second->actions()) {
        OnComplete(action, action_by_region.second, tries, server_name, region_result_itr->second,
                   failed_actions);
      }
    } else {
      auto region_exc = multi_response->RegionException(action_by_region.first);
      std::shared_ptr<std::exception> pexc;
      if (region_exc == nullptr) {
        VLOG(8) << "Server sent us neither results nor exceptions for " << action_by_region.first;
        pexc = std::make_shared<std::exception>(std::runtime_error("Invalid response"));
        // TODO: raise this exception to the application
      } else {
        // TODO HBASE-17800 for exc check with DoNotRetryIOException
        LogException(tries, action_by_region.second, region_exc, server_name);
        location_cache_->UpdateCachedLocation(*action_by_region.second->region_location(),
                                              *region_exc);
        std::string row_name;
        if (tries >= max_attempts_) {
          // Concurrent
          FailAll(action_by_region.second->actions(), tries, region_exc, server_name);
          return;
        }
        // Concurrent
        AddError(action_by_region.second->actions(), region_exc, server_name);
        for (const auto &action : action_by_region.second->actions()) {
          failed_actions.push_back(action);
        }
      }
    }
  }
  if (!failed_actions.empty()) {
    TryResubmit(failed_actions, tries);
  }
  return;
}

void AsyncBatchRpcRetryingCaller::OnComplete(const std::shared_ptr<Action> &action,
                                             const std::shared_ptr<RegionRequest> &region_request,
                                             int32_t tries,
                                             const std::shared_ptr<ServerName> &server_name,
                                             const std::shared_ptr<RegionResult> &region_result,
                                             std::vector<std::shared_ptr<Action>> &failed_actions) {
  std::string err_msg;
  try {
    auto result_or_exc = region_result->ResultOrException(action->original_index());
    auto result = std::get<0>(*result_or_exc);
    auto exc = std::get<1>(*result_or_exc);
    std::shared_ptr<std::exception> pexc;
    if (exc != nullptr) {
      LogException(tries, region_request, exc, server_name);
      if (tries >= max_attempts_) {
        FailOne(action, tries, exc, TimeUtil::GetNowNanos(), GetExtraContextForError(server_name));
      } else {
        failed_actions.push_back(action);
      }
    } else if (result != nullptr) {
      action2promises_[action->original_index()].setValue(std::move(result));
    } else {
      VLOG(8) << "Server " << server_name->ShortDebugString()
              << " sent us neither results nor exceptions for request @ index "
              << action->original_index() << ", row " << action->action()->row() << " of "
              << region_request->region_location()->region_name();
      err_msg = "Invalid response";
      AddError(action, std::make_shared<std::runtime_error>(err_msg), server_name);
      failed_actions.push_back(action);
    }
  } catch (const std::out_of_range &oor) {
    // TODO Feedback needed. Should we retry for he specific index again ?
    // This should never occur, so we are throwing a std::runtime_error from here
    VLOG(8) << "No ResultOrException found @ index " << action->original_index() << ", row "
            << action->action()->row() << " of "
            << region_request->region_location()->region_name();
    throw std::runtime_error("ResultOrException not present @ index " +
                             std::to_string(action->original_index()));
  }
  return;
}
} /* namespace hbase */
