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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Packaged MiniDFS regression, launched through bin/hbase with test dependencies in
 * HBASE_CLASSPATH.
 */
public final class MiniDFSClusterTool {

  private MiniDFSClusterTool() {
  }

  public static void main(String[] args) throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();
    util.getConfiguration().set("dfs.namenode.http-address", "127.0.0.1:0");
    util.getConfiguration().set("dfs.datanode.http.address", "127.0.0.1:0");
    util.getConfiguration().setInt("dfs.replication", 1);
    try {
      MiniDFSCluster cluster = util.startMiniDFSCluster(1);
      cluster.waitActive();
      System.out.println("MINIDFS_STARTED");
      try (FileSystem fs = cluster.getFileSystem()) {
        Path path = new Path("/jetty-regression");
        try (FSDataOutputStream output = fs.create(path)) {
          output.writeUTF("HBASE-30489");
        }
        try (FSDataInputStream input = fs.open(path)) {
          if (!"HBASE-30489".equals(input.readUTF())) {
            throw new AssertionError("MiniDFS read did not match the written data");
          }
        }
        if (!fs.delete(path, false)) {
          throw new AssertionError("Could not delete MiniDFS test data");
        }
        System.out.println("MINIDFS_READ_WRITE_OK");
      }
    } finally {
      util.shutdownMiniDFSCluster();
    }
    System.out.println("MINIDFS_CLOSED");
  }
}
