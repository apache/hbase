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
package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for exceptions thrown by an HBase server. May contain extra info about
 * the state of the server when the exception was thrown.
 */
@InterfaceAudience.Public
public class HBaseServerException extends HBaseIOException {
  private boolean serverOverloaded;

  public HBaseServerException() {
    this(false);
  }

  public HBaseServerException(String message) {
    this(false, message);
  }

  public HBaseServerException(boolean serverOverloaded) {
    this.serverOverloaded = serverOverloaded;
  }

  public HBaseServerException(boolean serverOverloaded, String message) {
    super(message);
    this.serverOverloaded = serverOverloaded;
  }

  /**
   * @param t throwable to check for server overloaded state
   * @return True if the server was considered overloaded when the exception was thrown
   */
  public static boolean isServerOverloaded(Throwable t) {
    if (t instanceof HBaseServerException) {
      return ((HBaseServerException) t).isServerOverloaded();
    }
    return false;
  }

  /**
   * Necessary for parsing RemoteException on client side
   * @param serverOverloaded True if server was overloaded when exception was thrown
   */
  public void setServerOverloaded(boolean serverOverloaded) {
    this.serverOverloaded = serverOverloaded;
  }

  /**
   * @return True if server was considered overloaded when exception was thrown
   */
  public boolean isServerOverloaded() {
    return serverOverloaded;
  }
}
