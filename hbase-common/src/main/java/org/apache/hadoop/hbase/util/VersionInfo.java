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

package org.apache.hadoop.hbase.util;

import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.hadoop.hbase.Version;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class finds the Version information for HBase.
 */
@InterfaceAudience.Public
public class VersionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class.getName());

  // If between two dots there is not a number, we regard it as a very large number so it is
  // higher than any numbers in the version.
  private static final int VERY_LARGE_NUMBER = 100000;

  /**
   * Get the hbase version.
   * @return the hbase version string, eg. "0.6.3-dev"
   */
  public static String getVersion() {
    return Version.version;
  }

  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return Version.revision;
  }

  /**
   * The date that hbase was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return Version.date;
  }

  /**
   * The user that compiled hbase.
   * @return the username of the user
   */
  public static String getUser() {
    return Version.user;
  }

  /**
   * Get the subversion URL for the root hbase directory.
   * @return the url
   */
  public static String getUrl() {
    return Version.url;
  }

  static String[] versionReport() {
    return new String[] {
      "HBase " + getVersion(),
      "Source code repository " + getUrl() + " revision=" + getRevision(),
      "Compiled by " + getUser() + " on " + getDate(),
      "From source with checksum " + getSrcChecksum()
      };
  }

  /**
   * Get the checksum of the source files from which Hadoop was compiled.
   * @return a string that uniquely identifies the source
   **/
  public static String getSrcChecksum() {
    return Version.srcChecksum;
  }

  public static void writeTo(PrintWriter out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

  public static void writeTo(PrintStream out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

  public static void logVersion() {
    for (String line : versionReport()) {
      LOG.info(line);
    }
  }

  public static int compareVersion(String v1, String v2) {
    //fast compare equals first
    if (v1.equals(v2)) {
      return 0;
    }

    Object[] v1Comps = getVersionComponents(v1); //1.2.3-hotfix -> [1, 2, 3, hotfix]
    Object[] v2Comps = getVersionComponents(v2);
    int index = 0;
    while (index < v1Comps.length && index < v2Comps.length) {
      int va = v1Comps[index] instanceof Integer ? (Integer)v1Comps[index] : VERY_LARGE_NUMBER;
      int vb = v2Comps[index] instanceof Integer ? (Integer)v2Comps[index] : VERY_LARGE_NUMBER;

      if (va != vb) {
        return va - vb;
      }
      if (va == VERY_LARGE_NUMBER) {
        // here, va and vb components must be same and Strings, compare as String
        int c = ((String)v1Comps[index]).compareTo((String)v2Comps[index]);
        if (c != 0) {
          return c;
        }
      }
      index++;
    }
    if (index < v1Comps.length) {
      // v1 is longer
      return 1;
    }
    //v2 is longer
    return -1;
  }

  /**
   * Returns the version components as Integer and String objects
   * Examples: "1.2.3" returns [1, 2, 3], "4.5.6-SNAPSHOT" returns [4, 5, 6, "SNAPSHOT"]
   * @return the components of the version string
   */
  static Object[] getVersionComponents(final String version) {
    assert(version != null);
    Object[] strComps = version.split("[\\.-]");
    assert(strComps.length > 0);

    Object[] comps = new Object[strComps.length];
    for (int i = 0; i < strComps.length; ++i) {
      try {
        comps[i] = Integer.parseInt((String) strComps[i]);
      } catch (NumberFormatException e) {
        comps[i] = strComps[i];
      }
    }
    return comps;
  }

  public static void main(String[] args) {
    writeTo(System.out);
  }
}
