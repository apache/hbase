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
#include "connection/sasl-util.h"

#include <glog/logging.h>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <sasl/saslutil.h>

#include <string>

int SaslUtil::GetPluginPath(void *context __attribute__((unused)), const char **path) {
  *path = getenv("SASL_PATH");

  if (*path == NULL) {
    *path = kDefaultPluginDir;
  }
  return SASL_OK;
}

void *SaslUtil::MutexNew(void) {
  auto m = new std::mutex();
  return m;
}

int SaslUtil::MutexLock(void *m) {
  (reinterpret_cast<std::mutex *>(m))->lock();
  return SASL_OK;
}

int SaslUtil::MutexUnlock(void *m) {
  (reinterpret_cast<std::mutex *>(m))->unlock();
  return SASL_OK;
}

void SaslUtil::MutexDispose(void *m) {
  std::mutex *mutex = reinterpret_cast<std::mutex *>(m);
  delete mutex;
}

std::once_flag SaslUtil::library_inited_;

void SaslUtil::InitializeSaslLib() {
  std::call_once(library_inited_, []() {
    sasl_set_mutex(reinterpret_cast<sasl_mutex_alloc_t *>(&SaslUtil::MutexNew),
                   reinterpret_cast<sasl_mutex_lock_t *>(&SaslUtil::MutexLock),
                   reinterpret_cast<sasl_mutex_unlock_t *>(&SaslUtil::MutexUnlock),
                   reinterpret_cast<sasl_mutex_free_t *>(&SaslUtil::MutexDispose));
    static sasl_callback_t callbacks[] = {
        {SASL_CB_GETPATH, (sasl_callback_ft)&SaslUtil::GetPluginPath, NULL},
        {SASL_CB_LIST_END, NULL, NULL}};
    int rc = sasl_client_init(callbacks);
    if (rc != SASL_OK) {
      throw std::runtime_error("Cannot initialize client " + std::to_string(rc));
    }
  });
}

std::string SaslUtil::ParseServiceName(std::shared_ptr<hbase::Configuration> conf, bool secure) {
  if (!secure) {
    return std::string();
  }
  std::string svrPrincipal = conf->Get(kServerPrincipalConfKey, "");
  // principal is of this form: hbase/23a03935850c@EXAMPLE.COM
  // where 23a03935850c is the host (optional)
  std::size_t pos = svrPrincipal.find("/");
  if (pos == std::string::npos && svrPrincipal.find("@") != std::string::npos) {
    pos = svrPrincipal.find("@");
  }
  if (pos == std::string::npos) {
    throw std::runtime_error("Couldn't retrieve service principal from conf");
  }
  VLOG(1) << "pos " << pos << " " << svrPrincipal;
  std::string service_name = svrPrincipal.substr(0, pos);
  return service_name;
}
