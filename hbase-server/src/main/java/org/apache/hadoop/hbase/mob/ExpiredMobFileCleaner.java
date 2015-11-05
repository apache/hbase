/**
 *
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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.protobuf.ServiceException;

/**
 * The cleaner to delete the expired MOB files.
 */
@InterfaceAudience.Private
public class ExpiredMobFileCleaner extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ExpiredMobFileCleaner.class);
  /**
   * Cleans the MOB files when they're expired and their min versions are 0.
   * If the latest timestamp of Cells in a MOB file is older than the TTL in the column family,
   * it's regarded as expired. This cleaner deletes them.
   * At a time T0, the cells in a mob file M0 are expired. If a user starts a scan before T0, those
   * mob cells are visible, this scan still runs after T0. At that time T1, this mob file M0
   * is expired, meanwhile a cleaner starts, the M0 is archived and can be read in the archive
   * directory.
   * @param tableName The current table name.
   * @param family The current family.
   * @throws ServiceException
   * @throws IOException
   */
  public void cleanExpiredMobFiles(String tableName, HColumnDescriptor family)
      throws ServiceException, IOException {
    Configuration conf = getConf();
    TableName tn = TableName.valueOf(tableName);
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Cleaning the expired MOB files of " + family.getNameAsString() + " in " + tableName);
    // disable the block cache.
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    CacheConfig cacheConfig = new CacheConfig(copyOfConf);
    MobUtils.cleanExpiredMobFiles(fs, conf, tn, family, cacheConfig,
        EnvironmentEdgeManager.currentTime());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    ToolRunner.run(conf, new ExpiredMobFileCleaner(), args);
  }

  private void printUsage() {
    System.err.println("Usage:\n" + "--------------------------\n"
        + ExpiredMobFileCleaner.class.getName() + " tableName familyName");
    System.err.println(" tableName        The table name");
    System.err.println(" familyName       The column family name");
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      return 1;
    }
    String tableName = args[0];
    String familyName = args[1];
    TableName tn = TableName.valueOf(tableName);
    HBaseAdmin.checkHBaseAvailable(getConf());
    Connection connection = ConnectionFactory.createConnection(getConf());
    Admin admin = connection.getAdmin();
    try {
      HTableDescriptor htd = admin.getTableDescriptor(tn);
      HColumnDescriptor family = htd.getFamily(Bytes.toBytes(familyName));
      if (family == null || !family.isMobEnabled()) {
        throw new IOException("Column family " + familyName + " is not a MOB column family");
      }
      if (family.getMinVersions() > 0) {
        throw new IOException(
            "The minVersions of the column family is not 0, could not be handled by this cleaner");
      }
      cleanExpiredMobFiles(tableName, family);
      return 0;
    } finally {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.error("Failed to close the HBaseAdmin.", e);
      }
      try {
        connection.close();
      } catch (IOException e) {
        LOG.error("Failed to close the connection.", e);
      }
    }
  }

}
