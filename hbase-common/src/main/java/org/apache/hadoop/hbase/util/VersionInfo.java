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
  private static int VERY_LARGE_NUMBER = 100000;

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

    String s1[] = v1.split("\\.|-");//1.2.3-hotfix -> [1, 2, 3, hotfix]
    String s2[] = v2.split("\\.|-");
    int index = 0;
    while (index < s1.length && index < s2.length) {
      int va = VERY_LARGE_NUMBER, vb = VERY_LARGE_NUMBER;
      try {
        va = Integer.parseInt(s1[index]);
      } catch (Exception ingore) {
      }
      try {
        vb = Integer.parseInt(s2[index]);
      } catch (Exception ingore) {
      }
      if (va != vb) {
        return va - vb;
      }
      if (va == VERY_LARGE_NUMBER) {
        // compare as String
        int c = s1[index].compareTo(s2[index]);
        if (c != 0) {
          return c;
        }
      }
      index++;
    }
    if (index < s1.length) {
      // s1 is longer
      return 1;
    }
    //s2 is longer
    return -1;
  }

  public static void main(String[] args) {
    writeTo(System.out);
  }
}
