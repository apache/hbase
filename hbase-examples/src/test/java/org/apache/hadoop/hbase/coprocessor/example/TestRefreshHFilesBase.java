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
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRefreshHFilesBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestRefreshHFilesBase.class);
  protected static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  protected static final int NUM_RS = 2;
  protected static final TableName TABLE_NAME = TableName.valueOf("testRefreshRegionHFilesEP");
  protected static final byte[] FAMILY = Bytes.toBytes("family");
  protected static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  protected static final byte[][] SPLIT_KEY = new byte[][] { Bytes.toBytes("30") };
  protected static final int NUM_ROWS = 5;
  protected static final String HFILE_NAME = "123abcdef";

  protected static Configuration CONF = HTU.getConfiguration();
  protected static MiniHBaseCluster cluster;
  protected static Table table;

  public static void setUp(String regionImpl) {
    try {
      CONF.set(HConstants.REGION_IMPL, regionImpl);
      CONF.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);

      CONF.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
              RefreshHFilesEndpoint.class.getName());
      cluster = HTU.startMiniCluster(NUM_RS);

      // Create table
      table = HTU.createTable(TABLE_NAME, FAMILY, SPLIT_KEY);

      // this will create 2 regions spread across slaves
      HTU.loadNumericRows(table, FAMILY, 1, 20);
      HTU.flush(TABLE_NAME);
    } catch (Exception ex) {
      LOG.error("Couldn't finish setup", ex);
    }
  }

  @After
  public void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  protected void addHFilesToRegions() throws IOException {
    MasterFileSystem mfs = HTU.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(), TABLE_NAME);
    for (Region region : cluster.getRegions(TABLE_NAME)) {
      Path regionDir = new Path(tableDir, region.getRegionInfo().getEncodedName());
      Path familyDir = new Path(regionDir, Bytes.toString(FAMILY));
      HFileTestUtil.createHFile(HTU.getConfiguration(), HTU.getTestFileSystem(),
              new Path(familyDir, HFILE_NAME), FAMILY, QUALIFIER, Bytes.toBytes("50"),
              Bytes.toBytes("60"), NUM_ROWS);
    }
  }
}
