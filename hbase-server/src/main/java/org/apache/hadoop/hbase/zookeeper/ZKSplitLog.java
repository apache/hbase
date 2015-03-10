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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.zookeeper.KeeperException;

/**
 * Common methods and attributes used by 
 * {@link org.apache.hadoop.hbase.master.SplitLogManager} and 
 * {@link org.apache.hadoop.hbase.regionserver.SplitLogWorker}
 * running distributed splitting of WAL logs.
 */
@InterfaceAudience.Private
public class ZKSplitLog {
  private static final Log LOG = LogFactory.getLog(ZKSplitLog.class);

  /**
   * Gets the full path node name for the log file being split.
   * This method will url encode the filename.
   * @param zkw zk reference
   * @param filename log file name (only the basename)
   */
  public static String getEncodedNodeName(ZooKeeperWatcher zkw, String filename) {
    return ZKUtil.joinZNode(zkw.splitLogZNode, encode(filename));
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

  public static String getRescanNode(ZooKeeperWatcher zkw) {
    return ZKUtil.joinZNode(zkw.splitLogZNode, "RESCAN");
  }

  /**
   * @param name the last part in path
   * @return whether the node name represents a rescan node
   */
  public static boolean isRescanNode(String name) {
    return name.startsWith("RESCAN");
  }

  /**
   * @param zkw
   * @param path the absolute path, starts with '/'
   * @return whether the path represents a rescan node
   */
  public static boolean isRescanNode(ZooKeeperWatcher zkw, String path) {
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

  public static boolean isTaskPath(ZooKeeperWatcher zkw, String path) {
    String dirname = path.substring(0, path.lastIndexOf('/'));
    return dirname.equals(zkw.splitLogZNode);
  }

  public static Path getSplitLogDir(Path rootdir, String tmpname) {
    return new Path(new Path(rootdir, HConstants.SPLIT_LOGDIR_NAME), tmpname);
  }


  public static String getSplitLogDirTmpComponent(final String worker, String file) {
    return worker + "_" + ZKSplitLog.encode(file);
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

  /*
   * Following methods come from SplitLogManager
   */

  /**
   * check if /hbase/recovering-regions/<current region encoded name> exists. Returns true if exists
   * and set watcher as well.
   * @param zkw
   * @param regionEncodedName region encode name
   * @return true when /hbase/recovering-regions/<current region encoded name> exists
   * @throws KeeperException
   */
  public static boolean
      isRegionMarkedRecoveringInZK(ZooKeeperWatcher zkw, String regionEncodedName)
          throws KeeperException {
    boolean result = false;
    String nodePath = ZKUtil.joinZNode(zkw.recoveringRegionsZNode, regionEncodedName);

    byte[] node = ZKUtil.getDataAndWatch(zkw, nodePath);
    if (node != null) {
      result = true;
    }
    return result;
  }

  /**
   * @param bytes - Content of a failed region server or recovering region znode.
   * @return long - The last flushed sequence Id for the region server
   */
  public static long parseLastFlushedSequenceIdFrom(final byte[] bytes) {
    long lastRecordedFlushedSequenceId = -1l;
    try {
      lastRecordedFlushedSequenceId = ZKUtil.parseWALPositionFrom(bytes);
    } catch (DeserializationException e) {
      lastRecordedFlushedSequenceId = -1l;
      LOG.warn("Can't parse last flushed sequence Id", e);
    }
    return lastRecordedFlushedSequenceId;
  }

  public static void deleteRecoveringRegionZNodes(ZooKeeperWatcher watcher, List<String> regions) {
    try {
      if (regions == null) {
        // remove all children under /home/recovering-regions
        LOG.debug("Garbage collecting all recovering region znodes");
        ZKUtil.deleteChildrenRecursively(watcher, watcher.recoveringRegionsZNode);
      } else {
        for (String curRegion : regions) {
          String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, curRegion);
          ZKUtil.deleteNodeRecursively(watcher, nodePath);
        }
      }
    } catch (KeeperException e) {
      LOG.warn("Cannot remove recovering regions from ZooKeeper", e);
    }
  }

  /**
   * This function is used in distributedLogReplay to fetch last flushed sequence id from ZK
   * @param zkw
   * @param serverName
   * @param encodedRegionName
   * @return the last flushed sequence ids recorded in ZK of the region for <code>serverName<code>
   * @throws IOException
   */

  public static RegionStoreSequenceIds getRegionFlushedSequenceId(ZooKeeperWatcher zkw,
      String serverName, String encodedRegionName) throws IOException {
    // when SplitLogWorker recovers a region by directly replaying unflushed WAL edits,
    // last flushed sequence Id changes when newly assigned RS flushes writes to the region.
    // If the newly assigned RS fails again(a chained RS failures scenario), the last flushed
    // sequence Id name space (sequence Id only valid for a particular RS instance), changes
    // when different newly assigned RS flushes the region.
    // Therefore, in this mode we need to fetch last sequence Ids from ZK where we keep history of
    // last flushed sequence Id for each failed RS instance.
    RegionStoreSequenceIds result = null;
    String nodePath = ZKUtil.joinZNode(zkw.recoveringRegionsZNode, encodedRegionName);
    nodePath = ZKUtil.joinZNode(nodePath, serverName);
    try {
      byte[] data;
      try {
        data = ZKUtil.getData(zkw, nodePath);
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
      if (data != null) {
        result = ZKUtil.parseRegionStoreSequenceIds(data);
      }
    } catch (KeeperException e) {
      throw new IOException("Cannot get lastFlushedSequenceId from ZooKeeper for server="
          + serverName + "; region=" + encodedRegionName, e);
    } catch (DeserializationException e) {
      LOG.warn("Can't parse last flushed sequence Id from znode:" + nodePath, e);
    }
    return result;
  }
}
