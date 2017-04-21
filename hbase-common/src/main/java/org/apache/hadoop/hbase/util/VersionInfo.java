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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Version;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This class finds the Version information for HBase.
 */
@InterfaceAudience.Public
public class VersionInfo {
  private static final Log LOG = LogFactory.getLog(VersionInfo.class.getName());

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

  public static void main(String[] args) {
    writeTo(System.out);
  }
}
