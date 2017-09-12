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

#include <memory>
#include <mutex>
#include <string>

#include "hbase/client/configuration.h"

class SaslUtil {
 public:
  void InitializeSaslLib(void);
  static std::string ParseServiceName(std::shared_ptr<hbase::Configuration> conf, bool secure);

 private:
  static constexpr const char *kDefaultPluginDir = "/usr/lib/sasl2";
  // for now the sasl handler is hardcoded to work against the regionservers only. In the future, if
  // we
  // need the master rpc to work, we could have a map of service names to principals to use (similar
  // to the Java implementation)
  static constexpr const char *kServerPrincipalConfKey = "hbase.regionserver.kerberos.principal";

  static int GetPluginPath(void *context, const char **path);
  static void *MutexNew(void);
  static int MutexLock(void *m);
  static int MutexUnlock(void *m);
  static void MutexDispose(void *m);
  static std::once_flag library_inited_;
};
