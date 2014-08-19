/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef HBASE_COMMON_STATUS_H__
#define HBASE_COMMON_STATUS_H__

#include <string>
#include <stdint.h>

#include "hbase_macros.h"

namespace hbase {

class Status {
public:
  Status(int32_t code, const char *msg="")
      : code_(code), msg_(msg) { }

  void SetCode(int32_t code) {code_ = code;}

  int32_t GetCode() { return code_; }

  bool ok() const { return code_ == 0; }

  static const Status Success;
  static const Status EInvalid;
  static const Status ENoEntry;
  static const Status ENoMem;
  static const Status ENoSys;
  static const Status ERange;
  static const Status EAgain;
  static const Status ENoBufs;
  static const Status EBusy;

  static const Status HBaseInternalError;
  static const Status HBaseTableDisabled;

private:
  int32_t     code_;
  std::string msg_;
};

} /* namespace hbase */

#endif /* HBASE_COMMON_STATUS_H__ */
