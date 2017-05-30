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

#include <exception>
#include <string>
#include <vector>
#include <folly/io/IOBuf.h>
#include <folly/ExceptionWrapper.h>

namespace hbase {

class ThrowableWithExtraContext {
public:
  ThrowableWithExtraContext(folly::exception_wrapper cause,
      const long& when) :
      cause_(cause), when_(when), extras_("") {
  }

  ThrowableWithExtraContext(folly::exception_wrapper cause,
      const long& when, const std::string& extras) :
      cause_(cause), when_(when), extras_(extras) {
  }

  virtual std::string ToString() {
    // TODO:
    // return new Date(this.when).toString() + ", " + extras + ", " + t.toString();
    return extras_ + ", " + cause_.what().toStdString();
  }

  virtual folly::exception_wrapper cause() {
    return cause_;
  }
private:
  folly::exception_wrapper cause_;
  long when_;
  std::string extras_;
};

class IOException: public std::logic_error {
public:
  IOException() : logic_error("") {}

  IOException(
        const std::string& what) :
        logic_error(what) {}
  IOException(
      const std::string& what,
	  folly::exception_wrapper cause) :
      logic_error(what), cause_(cause) {}
  virtual ~IOException() = default;

  virtual folly::exception_wrapper cause() {
    return cause_;
  }
private:
  folly::exception_wrapper cause_;
};

class RetriesExhaustedException: public IOException {
public:
  RetriesExhaustedException(
      const int& num_retries,
      std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions) :
        IOException(
            GetMessage(num_retries, exceptions),
            exceptions->empty() ? folly::exception_wrapper{}
              : (*exceptions)[exceptions->size() - 1].cause()){
  }
  virtual ~RetriesExhaustedException() = default;

private:
  std::string GetMessage(
      const int& num_retries,
      std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions) {
    std::string buffer("Failed after attempts=");
    buffer.append(std::to_string(num_retries + 1));
    buffer.append(", exceptions:\n");
    for (auto it = exceptions->begin();  it != exceptions->end(); it++) {
      buffer.append(it->ToString());
      buffer.append("\n");
    }
    return buffer;
  }
};

class HBaseIOException : public IOException {
};

class RemoteException : public IOException {
public:

  RemoteException() : port_(0), do_not_retry_(false) {}

  RemoteException(const std::string& what) :
      IOException(what), port_(0), do_not_retry_(false) {}

  RemoteException(
      const std::string& what,
      folly::exception_wrapper cause) :
      IOException(what, cause), port_(0), do_not_retry_(false) {}

  virtual ~RemoteException() = default;

  std::string exception_class_name() const {
    return exception_class_name_;
  }

  RemoteException* set_exception_class_name(const std::string& value) {
    exception_class_name_ = value;
    return this;
  }

  std::string stack_trace() const {
    return stack_trace_;
  }

  RemoteException* set_stack_trace(const std::string& value) {
    stack_trace_ = value;
    return this;
  }

  std::string hostname() const {
    return hostname_;
  }

  RemoteException* set_hostname(const std::string& value) {
    hostname_ = value;
    return this;
  }

  int port() const {
    return port_;
  }

  RemoteException* set_port(int value) {
    port_ = value;
    return this;
  }

  bool do_not_retry() const {
    return do_not_retry_;
  }

  RemoteException* set_do_not_retry(bool value) {
    do_not_retry_ = value;
    return this;
  }

private:
  std::string exception_class_name_;
  std::string stack_trace_;
  std::string hostname_;
  int port_;
  bool do_not_retry_;
};
}  // namespace hbase
