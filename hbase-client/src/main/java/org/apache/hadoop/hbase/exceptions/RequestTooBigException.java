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

package org.apache.hadoop.hbase.exceptions;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when the size of the rpc request received by the server is too large.
 *
 * On receiving such an exception, the client does not retry the offending rpc.
 * @since 1.3.0
 */
@InterfaceAudience.Public
public class RequestTooBigException extends DoNotRetryIOException {
  private static final long serialVersionUID = -1593339239809586516L;

  // Recognized only in HBase version 1.3 and higher.
  public static final int MAJOR_VERSION = 1;
  public static final int MINOR_VERSION = 3;

  public RequestTooBigException() {
    super();
  }

  public RequestTooBigException(String message) {
    super(message);
  }
}
