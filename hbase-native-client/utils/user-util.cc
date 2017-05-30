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

#include "utils/user-util.h"

#include <folly/Logging.h>
#include <krb5/krb5.h>
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

using namespace hbase;
using namespace std;

UserUtil::UserUtil() : once_flag_{} {}

string UserUtil::user_name(bool secure) {
  std::call_once(once_flag_, [this, secure]() { compute_user_name(secure); });
  return user_name_;
}

void UserUtil::compute_user_name(bool secure) {
  // According to the man page of getpwuid
  // this should never be free'd
  //
  // So yeah a raw pointer with no ownership....
  struct passwd *passwd = getpwuid(getuid());

  // make sure that we got something.
  if (passwd && passwd->pw_name) {
    user_name_ = string{passwd->pw_name};
  }
  if (!secure) return;
  krb5_context ctx;
  krb5_error_code ret = krb5_init_context(&ctx);
  if (ret != 0) {
    throw std::runtime_error("cannot init krb ctx " + std::to_string(ret));
  }
  krb5_ccache ccache;
  ret = krb5_cc_default(ctx, &ccache);
  if (ret != 0) {
    throw std::runtime_error("cannot get default cache " + std::to_string(ret));
  }
  // Here is sample principal: hbase/23a03935850c@EXAMPLE.COM
  // There may be one (user) or two (user/host) components before the @ sign
  krb5_principal princ;
  ret = krb5_cc_get_principal(ctx, ccache, &princ);
  if (ret != 0) {
    throw std::runtime_error("cannot get default principal " + std::to_string(ret));
  }
  user_name_ = princ->data->data;
  if (krb5_princ_size(ctx, princ) >= 2) {
    user_name_ += "/";
    user_name_ += static_cast<char *>(princ->data[1].data);
  }
  user_name_ += "@";
  user_name_ += princ->realm.data;
  VLOG(1) << "user " << user_name_;
  krb5_free_principal(ctx, princ);
  krb5_free_context(ctx);
}
