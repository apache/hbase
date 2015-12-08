/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;


/**
 * Class to help with parsing the version info.
 */
@InterfaceAudience.Private
public final class VersionInfoUtil {

  private VersionInfoUtil() {
    /* UTIL CLASS ONLY */
  }

  public static boolean currentClientHasMinimumVersion(int major, int minor) {
    RpcCallContext call = RpcServer.getCurrentCall();
    HBaseProtos.VersionInfo versionInfo = call != null ? call.getClientVersionInfo() : null;
    return hasMinimumVersion(versionInfo, major, minor);
  }

  public static boolean hasMinimumVersion(HBaseProtos.VersionInfo versionInfo,
                                          int major,
                                          int minor) {
    if (versionInfo != null) {
      try {
        String[] components = versionInfo.getVersion().split("\\.");

        int clientMajor = components.length > 0 ? Integer.parseInt(components[0]) : 0;
        if (clientMajor != major) {
          return clientMajor > major;
        }

        int clientMinor = components.length > 1 ? Integer.parseInt(components[1]) : 0;
        return clientMinor >= minor;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }
}
