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
package org.apache.hadoop.hbase.ipc;

import java.net.InetAddress;
import java.util.Optional;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;

/**
 * Interface of all necessary to carry out a RPC service invocation on the server. This interface
 * focus on the information needed or obtained during the actual execution of the service method.
 */
@InterfaceAudience.Private
public interface RpcCallContext {
  /**
   * Check if the caller who made this IPC call has disconnected. If called from outside the context
   * of IPC, this does nothing.
   * @return &lt; 0 if the caller is still connected. The time in ms since the disconnection
   *         otherwise
   */
  long disconnectSince();

  /**
   * If the client connected and specified a codec to use, then we will use this codec making
   * cellblocks to return. If the client did not specify a codec, we assume it does not support
   * cellblocks and will return all content protobuf'd (though it makes our serving slower). We need
   * to ask this question per call because a server could be hosting both clients that support
   * cellblocks while fielding requests from clients that do not.
   * @return True if the client supports cellblocks, else return all content in pb
   */
  boolean isClientCellBlockSupported();

  /**
   * Returns the user credentials associated with the current RPC request or not present if no
   * credentials were provided.
   * @return A User
   */
  Optional<User> getRequestUser();

  /**
   * @return Current request's user name or not present if none ongoing.
   */
  default Optional<String> getRequestUserName() {
    return getRequestUser().map(User::getShortName);
  }

  /**
   * @return Address of remote client in this call
   */
  InetAddress getRemoteAddress();

  /**
   * @return the client version info, or null if the information is not present
   */
  VersionInfo getClientVersionInfo();

  /**
   * Sets a callback which has to be executed at the end of this RPC call. Such a callback is an
   * optional one for any Rpc call. n
   */
  void setCallBack(RpcCallback callback);

  boolean isRetryImmediatelySupported();

  /**
   * The size of response cells that have been accumulated so far. This along with the corresponding
   * increment call is used to ensure that multi's or scans dont get too excessively large
   */
  long getResponseCellSize();

  /**
   * Add on the given amount to the retained cell size. This is not thread safe and not synchronized
   * at all. If this is used by more than one thread then everything will break. Since this is
   * called for every row synchronization would be too onerous.
   */
  void incrementResponseCellSize(long cellSize);

  long getResponseBlockSize();

  void incrementResponseBlockSize(long blockSize);

  long getResponseExceptionSize();

  void incrementResponseExceptionSize(long exceptionSize);
}
