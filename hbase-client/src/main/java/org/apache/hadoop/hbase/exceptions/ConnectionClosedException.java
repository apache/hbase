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

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when the connection is closed
 */

@InterfaceAudience.Public
public class ConnectionClosedException extends HBaseIOException {
  // TODO: Deprecate? Should inherit from DoNotRetryIOE but it does not.

  private static final long serialVersionUID = -8938225073412971497L;

  public ConnectionClosedException(String string) {
    super(string);
  }

  /**
   * ConnectionClosedException with cause
   *
   * @param message the message for this exception
   * @param cause the cause for this exception
   */
  public ConnectionClosedException(String message, Throwable cause) {
    super(message, cause);
  }
}
