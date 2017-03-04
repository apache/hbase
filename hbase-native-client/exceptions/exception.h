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

namespace hbase {

class ThrowableWithExtraContext {
public:
  ThrowableWithExtraContext(std::shared_ptr<std::exception> cause,
      const long& when) :
      cause_(cause), when_(when), extras_("") {
  }

  ThrowableWithExtraContext(std::shared_ptr<std::exception> cause,
      const long& when, const std::string& extras) :
      cause_(cause), when_(when), extras_(extras) {
  }

  std::string ToString() {
    // TODO:
    // return new Date(this.when).toString() + ", " + extras + ", " + t.toString();
    return extras_ + ", " + cause_->what();
  }

  std::shared_ptr<std::exception> cause() {
    return cause_;
  }
private:
  std::shared_ptr<std::exception> cause_;
  long when_;
  std::string extras_;
};

class IOException: public std::logic_error {
public:
  IOException(
        const std::string& what) :
        logic_error(what), cause_(nullptr) {}
  IOException(
      const std::string& what,
      std::shared_ptr<std::exception> cause) :
      logic_error(what), cause_(cause) {}
  virtual ~IOException() = default;

  std::shared_ptr<std::exception> cause() {
    return cause_;
  }
private:
  const std::shared_ptr<std::exception> cause_;
};

class RetriesExhaustedException: public IOException {
public:
  RetriesExhaustedException(
      const int& num_retries,
      std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions) :
        IOException(
            GetMessage(num_retries, exceptions),
            exceptions->empty() ? nullptr : (*exceptions)[exceptions->size() - 1].cause()){
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

class DoNotRetryIOException : public HBaseIOException {
};
}  // namespace hbase
