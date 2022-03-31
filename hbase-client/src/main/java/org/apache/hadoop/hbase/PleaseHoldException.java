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
 * This exception is thrown by the master when a region server was shut down and restarted so fast
 * that the master still hasn't processed the server shutdown of the first instance, or when master
 * is initializing and client call admin operations, or when an operation is performed on a region
 * server that is still starting.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
public class PleaseHoldException extends HBaseIOException {
  public PleaseHoldException(String message) {
    super(message);
  }

  public PleaseHoldException(String message, Throwable cause) {
    super(message, cause);
  }

  public PleaseHoldException(Throwable cause) {
    super(cause);
  }
}
