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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common methods and attributes used by SplitLogManager and SplitLogWorker running distributed
 * splitting of WAL logs.
 * @deprecated  since 2.4.0 and 3.0.0 replaced by procedure-based WAL splitting; see
 *    SplitWALManager.
 */
@Deprecated
@InterfaceAudience.Private
public final class ZKSplitLog {
  private static final Logger LOG = LoggerFactory.getLogger(ZKSplitLog.class);

  private ZKSplitLog() {
  }

  /**
   * Gets the full path node name for the log file being split.
   * This method will url encode the filename.
   * @param zkw zk reference
   * @param filename log file name (only the basename)
   */
  public static String getEncodedNodeName(ZKWatcher zkw, String filename) {
    return ZNodePaths.joinZNode(zkw.getZNodePaths().splitLogZNode, encode(filename));
  }

  public static String getFileName(String node) {
    String basename = node.substring(node.lastIndexOf('/') + 1);
    return decode(basename);
  }

  static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("URLENCODER doesn't support UTF-8");
    }
  }

  static String decode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("URLDecoder doesn't support UTF-8");
    }
  }

  public static String getRescanNode(ZKWatcher zkw) {
    return ZNodePaths.joinZNode(zkw.getZNodePaths().splitLogZNode, "RESCAN");
  }

  /**
   * @param name the last part in path
   * @return whether the node name represents a rescan node
   */
  public static boolean isRescanNode(String name) {
    return name.startsWith("RESCAN");
  }

  /**
   * Checks if the given path represents a rescan node.
   *
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and constants
   * @param path the absolute path, starts with '/'
   * @return whether the path represents a rescan node
   */
  public static boolean isRescanNode(ZKWatcher zkw, String path) {
    String prefix = getRescanNode(zkw);
    if (path.length() <= prefix.length()) {
      return false;
    }
    for (int i = 0; i < prefix.length(); i++) {
      if (prefix.charAt(i) != path.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  public static Path getSplitLogDir(Path rootdir, String tmpname) {
    return new Path(new Path(rootdir, HConstants.SPLIT_LOGDIR_NAME), tmpname);
  }

  public static void markCorrupted(Path rootdir, String logFileName,
      FileSystem fs) {
    Path file = new Path(getSplitLogDir(rootdir, logFileName), "corrupt");
    try {
      fs.createNewFile(file);
    } catch (IOException e) {
      LOG.warn("Could not flag a log file as corrupted. Failed to create " +
          file, e);
    }
  }

  public static boolean isCorrupted(Path rootdir, String logFileName,
      FileSystem fs) throws IOException {
    Path file = new Path(getSplitLogDir(rootdir, logFileName), "corrupt");
    boolean isCorrupt;
    isCorrupt = fs.exists(file);
    return isCorrupt;
  }
}
