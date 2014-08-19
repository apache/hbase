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
#ifndef HBASE_JNI_IMPL_CLIENT_H__
#define HBASE_JNI_IMPL_CLIENT_H__

#ifndef _WIN32
#include <pthread.h>
#endif

#include "hbase_config.h"
#include "hbase_get.h"
#include "hbase_mutations.h"
#include "hbase_status.h"
#include "jnihelper.h"

namespace hbase {

class HBaseClient : public JniObject {
public:
  HBaseClient();

  ~HBaseClient();

  Status Init(HBaseConfiguration *conf, JNIEnv *current_env=NULL);

  Status Flush(hb_client_disconnection_cb cb, void *extra, JNIEnv *current_env=NULL);

  Status Close(hb_client_disconnection_cb cb, void *extra, JNIEnv *current_env=NULL);

  Status SendMutation(Mutation *mutation, hb_mutation_cb cb, void *extra, JNIEnv *current_env=NULL);

  Status SendGet(Get *get, hb_mutation_cb cb, void *extra, JNIEnv *current_env=NULL);

private:
  pthread_mutex_t client_mutex;
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_CLIENT_H__ */
