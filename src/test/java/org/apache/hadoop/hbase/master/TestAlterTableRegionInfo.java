/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.master;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Category(MediumTests.class)
public class TestAlterTableRegionInfo {
  private final Log LOG = LogFactory.getLog(TestAlterTableLocking.class);
  private final static HBaseTestingUtility testingUtility = new HBaseTestingUtility();
  private static final byte[] TABLENAME = Bytes.toBytes("TestTable");
  private static final byte[] FAMILY1NAME = Bytes.toBytes("cf1");
  private static final byte[] FAMILY2NAME = Bytes.toBytes("cf2");
  private static final int NUM_REGIONS = 20;
  private static final int NUM_VERSIONS = 1;
  private static final byte[] START_KEY = Bytes.toBytes(0);
  private static final byte[] END_KEY = Bytes.toBytes(200);
  private static final int WAIT_INTERVAL = 1000;
  private static final int MAX_CONCURRENT_REGIONS_CLOSED = 2;
  private static final int WAIT_FOR_TABLE = 30 * 1000;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    //Start mini cluster
    testingUtility.startMiniCluster(3);
    testingUtility.createTable(
        TABLENAME,
        new byte[][]{ FAMILY1NAME, FAMILY2NAME },
        NUM_VERSIONS,
        START_KEY,
        END_KEY,
        NUM_REGIONS
        );
    testingUtility.waitTableAvailable(TABLENAME, WAIT_FOR_TABLE);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    testingUtility.shutdownMiniCluster();
  }

  @Test
  public void testRegionInfoConsistency() throws IOException {
    HBaseAdmin admin = null;
    admin = testingUtility.getHBaseAdmin();

    List<HColumnDescriptor> columnAdditions = new ArrayList<>();
    columnAdditions.add(new HColumnDescriptor("cf3"));

    List<Pair<byte[], HColumnDescriptor>> columnModifications = new ArrayList<>();
    columnModifications.add(new Pair<byte[], HColumnDescriptor>(Bytes.toBytes("cf1"),
        new HColumnDescriptor("cf1modified")));

    List<byte[]> columnDeletions = new ArrayList<>();
    columnDeletions.add(Bytes.toBytes("cf2"));

    admin.alterTable(TABLENAME, columnAdditions, columnModifications, columnDeletions,
      WAIT_INTERVAL, MAX_CONCURRENT_REGIONS_CLOSED);
    Set<byte[]> families = admin.getTableDescriptor(TABLENAME).getFamiliesKeys();

    // each entry in allReadFamilies contains the set of families of a particular .regioninfo file
    List<Set<byte[]>> allReadFamilies = readAllRegionInfoFamilies(admin, TABLENAME);
    for (Set<byte[]> readFamilies : allReadFamilies) {
      // each set of families must be correct
      Assert.assertEquals("Expected was " + families + ", got " + readFamilies + ".", families,
        readFamilies);
    }
  }

  /**
   * This methods reads all the .regioninfo file of every region in a particular table and finds the
   * set of families in each file
   * @param admin
   * @param tableName -- name of table to be examined
   * @return List of Sets. Each Set contains the column family names of a particular region
   * @throws IOException
   */
  List<Set<byte[]>> readAllRegionInfoFamilies(HBaseAdmin admin, byte[] tableName)
      throws IOException {
    List<Set<byte[]>> allReadFamilies = new ArrayList<Set<byte[]>>();
    Configuration conf = admin.getConnection().getConf();
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);
    HTable table = new HTable(conf, tableName);
    byte[] tableDescName = table.getTableDescriptor().getName();

    // iterate through all of the table's regions and read their .regioninfo files
    for (HRegionInfo hri : table.getRegionsInfo().keySet()) {
      Path tableDir = HTableDescriptor.getTableDir(FSUtils.getRootDir(conf), tableDescName);
      Path regionPath = HRegion.getRegionDir(tableDir, hri.getEncodedName());
      Path regionInfoPath = new Path(regionPath, HRegion.REGIONINFO_FILE);

      FSDataInputStream in = fs.open(regionInfoPath);
      HRegionInfo f_hri = new HRegionInfo();
      try {
        f_hri.readFields(in);
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        in.close();
      }

      allReadFamilies.add(f_hri.getTableDesc().getFamiliesKeys());
    }
    table.close();
    return allReadFamilies;
  }


}
