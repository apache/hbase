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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.ipc.RemoteException;

/**
 * A {@link RemoteException} with some extra information.  If source exception
 * was a {@link DoNotRetryIOException}, {@link #isDoNotRetry()} will return true.
 * <p>A {@link RemoteException} hosts exceptions we got from the server.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RemoteWithExtrasException extends RemoteException {
  private final String hostname;
  private final int port;
  private final boolean doNotRetry;

  public RemoteWithExtrasException(String className, String msg, final boolean doNotRetry) {
    this(className, msg, null, -1, doNotRetry);
  }

  public RemoteWithExtrasException(String className, String msg, final String hostname,
      final int port, final boolean doNotRetry) {
    super(className, msg);
    this.hostname = hostname;
    this.port = port;
    this.doNotRetry = doNotRetry;
  }

  /**
   * @return null if not set
   */
  public String getHostname() {
    return this.hostname;
  }

  /**
   * @return -1 if not set
   */
  public int getPort() {
    return this.port;
  }

  /**
   * @return True if origin exception was a do not retry type.
   */
  public boolean isDoNotRetry() {
    return this.doNotRetry;
  }
}
