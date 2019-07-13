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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;


/**
 * Class to help with parsing the version info.
 */
@InterfaceAudience.Private
public final class VersionInfoUtil {

  private VersionInfoUtil() {
    /* UTIL CLASS ONLY */
  }

  public static boolean currentClientHasMinimumVersion(int major, int minor) {
    return hasMinimumVersion(getCurrentClientVersionInfo(), major, minor);
  }

  public static boolean hasMinimumVersion(HBaseProtos.VersionInfo versionInfo,
                                          int major,
                                          int minor) {
    if (versionInfo != null) {
      if (versionInfo.hasVersionMajor() && versionInfo.hasVersionMinor()) {
        int clientMajor = versionInfo.getVersionMajor();
        if (clientMajor != major) {
          return clientMajor > major;
        }
        int clientMinor = versionInfo.getVersionMinor();
        return clientMinor >= minor;
      }
      try {
        final String[] components = getVersionComponents(versionInfo);

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

  /**
   * @return the versionInfo extracted from the current RpcCallContext
   */
  public static HBaseProtos.VersionInfo getCurrentClientVersionInfo() {
    return RpcServer.getCurrentCall().map(RpcCallContext::getClientVersionInfo).orElse(null);
  }


  /**
   * @param version
   * @return the passed-in <code>version</code> int as a version String
   *         (e.g. 0x0103004 is 1.3.4)
   */
  public static String versionNumberToString(final int version) {
    return String.format("%d.%d.%d",
        ((version >> 20) & 0xff),
        ((version >> 12) & 0xff),
        (version & 0xfff));
  }

  /**
   * Pack the full number version in a int. by shifting each component by 8bit,
   * except the dot release which has 12bit.
   * Examples: (1.3.4 is 0x0103004, 2.1.0 is 0x0201000)
   * @param versionInfo the VersionInfo object to pack
   * @return the version number as int. (e.g. 0x0103004 is 1.3.4)
   */
  public static int getVersionNumber(final HBaseProtos.VersionInfo versionInfo) {
    if (versionInfo != null) {
      try {
        final String[] components = getVersionComponents(versionInfo);
        int clientMajor = components.length > 0 ? Integer.parseInt(components[0]) : 0;
        int clientMinor = components.length > 1 ? Integer.parseInt(components[1]) : 0;
        int clientPatch = components.length > 2 ? Integer.parseInt(components[2]) : 0;
        return buildVersionNumber(clientMajor, clientMinor, clientPatch);
      } catch (NumberFormatException e) {
        int clientMajor = versionInfo.hasVersionMajor() ? versionInfo.getVersionMajor() : 0;
        int clientMinor = versionInfo.hasVersionMinor() ? versionInfo.getVersionMinor() : 0;
        return buildVersionNumber(clientMajor, clientMinor, 0);
      }
    }
    return(0); // no version
  }

  /**
   * Pack the full number version in a int. by shifting each component by 8bit,
   * except the dot release which has 12bit.
   * Examples: (1.3.4 is 0x0103004, 2.1.0 is 0x0201000)
   * @param major version major number
   * @param minor version minor number
   * @param patch version patch number
   * @return the version number as int. (e.g. 0x0103004 is 1.3.4)
   */
  private static int buildVersionNumber(int major, int minor, int patch) {
    return (major << 20) | (minor << 12) | patch;
  }

  /**
   * Returns the version components
   * Examples: "1.4.3" returns [1, 4, 3], "4.5.6-SNAPSHOT" returns [4, 5, 6, "SNAPSHOT"]
   * @return the components of the version string
   */
  private static String[] getVersionComponents(final HBaseProtos.VersionInfo versionInfo) {
    return versionInfo.getVersion().split("[\\.-]");
  }
}
