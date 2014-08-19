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
#ifndef LIBHBASE_HBASE_H_
#define LIBHBASE_HBASE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "admin.h"
#include "client.h"
#include "coldesc.h"
#include "connection.h"
#include "get.h"
#include "log.h"
#include "mutations.h"
#include "result.h"
#include "scanner.h"
#include "types.h"

/*
 * This header file describes the APIs and data structures of a C client for
 * Apache HBase.
 *
 * Following conventions are used.
 *
 * 1. All data types are prefixed with the 'hb_'.
 *
 * 2. All exported functions are annotated with HBASE_API, prefixed with 'hb_'
 *    and named using the following convention:
 *    'hb_<subject>_<operation>_[<object>|<property>]'.
 *
 * 3. All asynchronous APIs take a callback which is triggered when the request
 *    completes. This callback can be triggered in the callers thread or another
 *    thread. To avoid any potential deadlock/starvation, applications should not
 *    block in the callback routine.
 *
 * 4. All callbacks take a void pointer for the user to supply their own data
 *    which is passed when callback is triggered.
 *
 * 4. Its applications responsibility to free up all the backing data buffers.
 *    Asynchronous APIs do not make a copy of data buffers hence the ownership
 *    of these buffers is temporarily transfered to the library until the
 *    callback is triggered. Application should not modify these buffers while
 *    they are in flight.
 *
 * 5. No explicit batching is supported for asynchronous APIs.
 *  ...
 */

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* LIBHBASE_HBASE_H_ */
