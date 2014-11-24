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

import org.apache.commons.logging.LogFactory;
import java.io.PrintWriter;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.VersionAnnotation;
import org.apache.commons.logging.Log;

/**
 * This class finds the package info for hbase and the VersionAnnotation
 * information.  Taken from hadoop.  Only name of annotation is different.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class VersionInfo {
  private static final Log LOG = LogFactory.getLog(VersionInfo.class.getName());
  private static Package myPackage;
  private static VersionAnnotation version;

  static {
    myPackage = VersionAnnotation.class.getPackage();
    version = myPackage.getAnnotation(VersionAnnotation.class);
  }

  /**
   * Get the meta-data for the hbase package.
   * @return package
   */
  static Package getPackage() {
    return myPackage;
  }

  /**
   * Get the hbase version.
   * @return the hbase version string, eg. "0.6.3-dev"
   */
  public static String getVersion() {
    return version != null ? version.version() : "Unknown";
  }

  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return version != null ? version.revision() : "Unknown";
  }

  /**
   * The date that hbase was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return version != null ? version.date() : "Unknown";
  }

  /**
   * The user that compiled hbase.
   * @return the username of the user
   */
  public static String getUser() {
    return version != null ? version.user() : "Unknown";
  }

  /**
   * Get the subversion URL for the root hbase directory.
   * @return the url
   */
  public static String getUrl() {
    return version != null ? version.url() : "Unknown";
  }

  static String[] versionReport() {
    return new String[] {
      "HBase " + getVersion(),
      "Source code repository " + getUrl() + " -r " + getRevision(),
      "Compiled by " + getUser() + " on " + getDate(),
      "From source with checksum " + getSrcChecksum()
      };
  }

  /**
   * Get the checksum of the source files from which Hadoop was compiled.
   * @return a string that uniquely identifies the source
   **/
  public static String getSrcChecksum() {
    return version != null ? version.srcChecksum() : "Unknown";
  }

  public static void writeTo(PrintWriter out) {
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
    logVersion();
  }
}
