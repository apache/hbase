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
 */
package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * When a fatal issue is encountered during an RPC which is not retriable, and the issue is
 * encountered deep in a call stack where throwing a checked DoNotRetryIOException is not possible
 * (e.g filter/comparator application), this unchecked exception can be thrown and will be wrapped
 * in a checked DoNotRetryIOException at the RPC boundary before returning to the client to prevent
 * client retries and to propagate the exception message cleanly. You should use this exception only
 * when absolutely necessary. Wherever possible, use a checked DoNotRetryIOException
 */
@InterfaceAudience.Private
public class DoNotRetryRuntimeException extends RuntimeException {
  public DoNotRetryRuntimeException(String message) {
    super(message);
  }
}
