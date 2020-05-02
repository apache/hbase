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
package org.apache.hadoop.hbase.io.asyncfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AsyncFSTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncFSTestBase.class);

  protected static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  protected static File CLUSTER_TEST_DIR;

  protected static MiniDFSCluster CLUSTER;

  private static boolean deleteOnExit() {
    String v = System.getProperty("hbase.testing.preserve.testdir");
    // Let default be true, to delete on exit.
    return v == null ? true : !Boolean.parseBoolean(v);
  }

  /**
   * Creates a directory for the cluster, under the test data
   */
  protected static void setupClusterTestDir() {
    // Using randomUUID ensures that multiple clusters can be launched by
    // a same test, if it stops & starts them
    Path testDir =
      UTIL.getDataTestDir("cluster_" + HBaseCommonTestingUtility.getRandomUUID().toString());
    CLUSTER_TEST_DIR = new File(testDir.toString()).getAbsoluteFile();
    // Have it cleaned up on exit
    boolean b = deleteOnExit();
    if (b) {
      CLUSTER_TEST_DIR.deleteOnExit();
    }
    LOG.info("Created new mini-cluster data directory: {}, deleteOnExit={}", CLUSTER_TEST_DIR, b);
  }

  private static String createDirAndSetProperty(final String property) {
    return createDirAndSetProperty(property, property);
  }

  private static String createDirAndSetProperty(final String relPath, String property) {
    String path = UTIL.getDataTestDir(relPath).toString();
    System.setProperty(property, path);
    UTIL.getConfiguration().set(property, path);
    new File(path).mkdirs();
    LOG.info("Setting " + property + " to " + path + " in system properties and HBase conf");
    return path;
  }

  private static void createDirsAndSetProperties() throws IOException {
    setupClusterTestDir();
    System.setProperty("test.build.data", CLUSTER_TEST_DIR.getPath());
    createDirAndSetProperty("test.cache.data");
    createDirAndSetProperty("hadoop.tmp.dir");

    // Frustrate yarn's and hdfs's attempts at writing /tmp.
    // Below is fragile. Make it so we just interpolate any 'tmp' reference.
    createDirAndSetProperty("dfs.journalnode.edits.dir");
    createDirAndSetProperty("dfs.datanode.shared.file.descriptor.paths");
    createDirAndSetProperty("nfs.dump.dir");
    createDirAndSetProperty("java.io.tmpdir");
    createDirAndSetProperty("dfs.journalnode.edits.dir");
    createDirAndSetProperty("dfs.provided.aliasmap.inmemory.leveldb.dir");
    createDirAndSetProperty("fs.s3a.committer.staging.tmp.path");
  }

  protected static void startMiniDFSCluster(int servers) throws IOException {
    if (CLUSTER != null) {
      throw new IllegalStateException("Already started");
    }
    createDirsAndSetProperties();

    Configuration conf = UTIL.getConfiguration();
    // Error level to skip some warnings specific to the minicluster. See HBASE-4709
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class)
      .setLevel(org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class)
      .setLevel(org.apache.log4j.Level.ERROR);

    TraceUtil.initTracer(conf);
    CLUSTER = new MiniDFSCluster.Builder(conf).numDataNodes(servers).build();
    CLUSTER.waitClusterUp();
  }

  protected static void shutdownMiniDFSCluster() {
    if (CLUSTER != null) {
      CLUSTER.shutdown(true);
      CLUSTER = null;
    }
  }
}
