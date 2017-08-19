/**
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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when server finds fatal issue w/ connection setup: e.g. bad rpc version
 * or unsupported auth method.
 * Closes connection after throwing this exception with message on why the failure.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
public class FatalConnectionException extends DoNotRetryIOException {
  public FatalConnectionException() {
    super();
  }

  public FatalConnectionException(String msg) {
    super(msg);
  }

  public FatalConnectionException(String msg, Throwable t) {
    super(msg, t);
  }
}
