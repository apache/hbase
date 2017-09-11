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
package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Passed as Exception by {@link ServerCrashProcedure}
 * notifying on-going RIT that server has failed. This exception is less an error-condition than
 * it is a signal to waiting procedures that they can now proceed.
 */
@InterfaceAudience.Private
@SuppressWarnings("serial")
public class ServerCrashException extends HBaseIOException {
  private final long procId;
  private final ServerName serverName;

  /**
   * @param serverName The server that crashed.
   */
  public ServerCrashException(long procId, ServerName serverName) {
    this.procId = procId;
    this.serverName = serverName;
  }

  @Override
  public String getMessage() {
    return "ServerCrashProcedure pid=" + this.procId + ", server=" + this.serverName;
  }
}
