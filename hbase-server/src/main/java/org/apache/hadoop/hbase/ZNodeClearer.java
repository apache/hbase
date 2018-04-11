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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Contains a set of methods for the collaboration between the start/stop scripts and the
 * servers. It allows to delete immediately the znode when the master or the regions server crashes.
 * The region server / master writes a specific file when it starts / becomes main master. When they
 * end properly, they delete the file.</p>
 * <p>In the script, we check for the existence of these files when the program ends. If they still
 * exist we conclude that the server crashed, likely without deleting their znode. To have a faster
 * recovery we delete immediately the znode.</p>
 * <p>The strategy depends on the server type. For a region server we store the znode path in the
 * file, and use it to delete it. for a master, as the znode path constant whatever the server, we
 * check its content to make sure that the backup server is not now in charge.</p>
 */
@InterfaceAudience.Private
public final class ZNodeClearer {
  private static final Logger LOG = LoggerFactory.getLogger(ZNodeClearer.class);

  private ZNodeClearer() {}

  /**
   * Logs the errors without failing on exception.
   */
  public static void writeMyEphemeralNodeOnDisk(String fileContent) {
    String fileName = ZNodeClearer.getMyEphemeralNodeFileName();
    if (fileName == null) {
      LOG.warn("Environment variable HBASE_ZNODE_FILE not set; znodes will not be cleared " +
          "on crash by start scripts (Longer MTTR!)");
      return;
    }

    FileWriter fstream;
    try {
      fstream = new FileWriter(fileName);
    } catch (IOException e) {
      LOG.warn("Can't write znode file "+fileName, e);
      return;
    }

    BufferedWriter out = new BufferedWriter(fstream);

    try {
      try {
        out.write(fileContent + "\n");
      } finally {
        try {
          out.close();
        } finally {
          fstream.close();
        }
      }
    } catch (IOException e) {
      LOG.warn("Can't write znode file "+fileName, e);
    }
  }

  /**
   * read the content of znode file, expects a single line.
   */
  public static String readMyEphemeralNodeOnDisk() throws IOException {
    String fileName = getMyEphemeralNodeFileName();
    if (fileName == null){
      throw new FileNotFoundException("No filename; set environment variable HBASE_ZNODE_FILE");
    }
    FileReader znodeFile = new FileReader(fileName);
    BufferedReader br = null;
    try {
      br = new BufferedReader(znodeFile);
      String file_content = br.readLine();
      return file_content;
    } finally {
      if (br != null) br.close();
    }
  }

  /**
   * Get the name of the file used to store the znode contents
   */
  public static String getMyEphemeralNodeFileName() {
    return System.getenv().get("HBASE_ZNODE_FILE");
  }

  /**
   *  delete the znode file
   */
  public static void deleteMyEphemeralNodeOnDisk() {
    String fileName = getMyEphemeralNodeFileName();

    if (fileName != null) {
      new File(fileName).delete();
    }
  }

  /**
   * See HBASE-14861. We are extracting master ServerName from rsZnodePath
   * example: "/hbase/rs/server.example.com,16020,1448266496481"
   * @param rsZnodePath from HBASE_ZNODE_FILE
   * @return String representation of ServerName or null if fails
   */

  public static String parseMasterServerName(String rsZnodePath) {
    String masterServerName = null;
    try {
      String[] rsZnodeParts = rsZnodePath.split("/");
      masterServerName = rsZnodeParts[rsZnodeParts.length -1];
    } catch (IndexOutOfBoundsException e) {
      LOG.warn("String " + rsZnodePath + " has wrong format", e);
    }
    return masterServerName;
  }

  /**
   *
   * @return true if cluster is configured with master-rs collocation
   */
  private static boolean tablesOnMaster(Configuration conf) {
    boolean tablesOnMaster = true;
    String confValue = conf.get(BaseLoadBalancer.TABLES_ON_MASTER);
    if (confValue != null && confValue.equalsIgnoreCase("none")) {
      tablesOnMaster = false;
    }
    return tablesOnMaster;
  }

  /**
   * Delete the master znode if its content (ServerName string) is the same
   *  as the one in the znode file. (env: HBASE_ZNODE_FILE). I case of master-rs
   *  colloaction we extract ServerName string from rsZnode path.(HBASE-14861)
   * @return true on successful deletion, false otherwise.
   */
  public static boolean clear(Configuration conf) {
    Configuration tempConf = new Configuration(conf);
    tempConf.setInt("zookeeper.recovery.retry", 0);

    ZKWatcher zkw;
    try {
      zkw = new ZKWatcher(tempConf, "clean znode for master",
          new Abortable() {
            @Override public void abort(String why, Throwable e) {}
            @Override public boolean isAborted() { return false; }
          });
    } catch (IOException e) {
      LOG.warn("Can't connect to zookeeper to read the master znode", e);
      return false;
    }

    String znodeFileContent;
    try {
      znodeFileContent = ZNodeClearer.readMyEphemeralNodeOnDisk();
      if (ZNodeClearer.tablesOnMaster(conf)) {
        // In case of master crash also remove rsZnode since master is also regionserver
        ZKUtil.deleteNodeFailSilent(zkw,
          ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, znodeFileContent));
        return MasterAddressTracker.deleteIfEquals(zkw,
          ZNodeClearer.parseMasterServerName(znodeFileContent));
      } else {
        return MasterAddressTracker.deleteIfEquals(zkw, znodeFileContent);
      }
    } catch (FileNotFoundException fnfe) {
      // If no file, just keep going -- return success.
      LOG.warn("Can't find the znode file; presume non-fatal", fnfe);
      return true;
    } catch (IOException e) {
      LOG.warn("Can't read the content of the znode file", e);
      return false;
    } catch (KeeperException e) {
      LOG.warn("ZooKeeper exception deleting znode", e);
      return false;
    } finally {
      zkw.close();
    }
  }
}
