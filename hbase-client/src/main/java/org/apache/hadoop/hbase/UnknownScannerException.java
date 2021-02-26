/**
 *
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
 * Thrown if a region server is passed an unknown scanner ID.
 * This usually means that the client has taken too long between checkins and so the
 * scanner lease on the server-side has expired OR the server-side is closing
 * down and has cancelled all leases.
 */
@InterfaceAudience.Public
public class UnknownScannerException extends DoNotRetryIOException {
  private static final long serialVersionUID = 993179627856392526L;

  public UnknownScannerException() {
    super();
  }

  /**
   * @param message the message for this exception
   */
  public UnknownScannerException(String message) {
    super(message);
  }

  /**
   * @param message the message for this exception
   * @param exception the exception to grab data from
   */
  public UnknownScannerException(String message, Exception exception) {
    super(message, exception);
  }
}
