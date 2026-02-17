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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MetricsRegionWrapperImpl;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestReopenTableRegionsIntegration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReopenTableRegionsIntegration.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("testLazyUpdateReopen");
  private static final byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLazyUpdateThenReopenUpdatesTableDescriptorHash() throws Exception {
    // Step 1: Create table with column family and 3 regions
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(CF).setMaxVersions(1).build();

    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(cfd)
      .setMaxFileSize(100 * 1024 * 1024L).build();

    UTIL.getAdmin().createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 3);
    UTIL.waitTableAvailable(TABLE_NAME);

    try {
      // Step 2: Capture initial tableDescriptorHash from all regions
      List<HRegion> regions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
      assertEquals("Expected 3 regions", 3, regions.size());

      Map<byte[], String> initialHashes = new HashMap<>();

      for (HRegion region : regions) {
        String hash;
        try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
          hash = wrapper.getTableDescriptorHash();
        }
        initialHashes.put(region.getRegionInfo().getRegionName(), hash);
      }

      // Verify all regions have same hash
      Set<String> uniqueHashes = new HashSet<>(initialHashes.values());
      assertEquals("All regions should have same hash", 1, uniqueHashes.size());
      String initialHash = uniqueHashes.iterator().next();

      // Step 3: Perform lazy table descriptor update
      ColumnFamilyDescriptor newCfd =
        ColumnFamilyDescriptorBuilder.newBuilder(cfd).setMaxVersions(5).build();

      TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td).modifyColumnFamily(newCfd)
        .setMaxFileSize(200 * 1024 * 1024L).build();

      // Perform lazy update (reopenRegions = false)
      UTIL.getAdmin().modifyTableAsync(newTd, false).get();

      // Wait for modification to complete
      UTIL.waitFor(30000, () -> {
        try {
          TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(TABLE_NAME);
          return currentTd.getMaxFileSize() == 200 * 1024 * 1024L;
        } catch (Exception e) {
          return false;
        }
      });

      // Step 4: Verify tableDescriptorHash has NOT changed in region metrics
      List<HRegion> regionsAfterLazyUpdate = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
      for (HRegion region : regionsAfterLazyUpdate) {
        String currentHash;
        try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
          currentHash = wrapper.getTableDescriptorHash();
        }
        assertEquals("Hash should NOT change without region reopen",
          initialHashes.get(region.getRegionInfo().getRegionName()), currentHash);
      }

      // Verify the table descriptor itself has changed
      TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(TABLE_NAME);
      String newDescriptorHash = currentTd.getDescriptorHash();
      assertNotEquals("Table descriptor should have new hash", initialHash, newDescriptorHash);

      // Step 5: Use new Admin API to reopen all regions
      UTIL.getAdmin().reopenTableRegions(TABLE_NAME);

      // Wait for all regions to be reopened
      UTIL.waitFor(60000, () -> {
        try {
          List<HRegion> currentRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
          if (currentRegions.size() != 3) {
            return false;
          }

          // Check if all regions now have the new hash
          for (HRegion region : currentRegions) {
            String hash;
            try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
              hash = wrapper.getTableDescriptorHash();
            }
            if (hash.equals(initialHash)) {
              return false;
            }
          }
          return true;
        } catch (Exception e) {
          return false;
        }
      });

      // Step 6: Verify tableDescriptorHash HAS changed in all region metrics
      List<HRegion> reopenedRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
      assertEquals("Should still have 3 regions", 3, reopenedRegions.size());

      for (HRegion region : reopenedRegions) {
        String currentHash;
        try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
          currentHash = wrapper.getTableDescriptorHash();
        }
        assertNotEquals("Hash SHOULD change after region reopen", initialHash, currentHash);
        assertEquals("Hash should match current table descriptor", newDescriptorHash, currentHash);
      }

      // Verify all regions show the same new hash
      Set<String> newHashes = new HashSet<>();
      for (HRegion region : reopenedRegions) {
        try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
          newHashes.add(wrapper.getTableDescriptorHash());
        }
      }
      assertEquals("All regions should have same new hash", 1, newHashes.size());

    } finally {
      UTIL.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testLazyUpdateThenReopenSpecificRegions() throws Exception {
    TableName tableName = TableName.valueOf("testSpecificRegionsReopen");

    // Step 1: Create table with 5 regions
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(CF).setMaxVersions(1).build();

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cfd)
      .setMaxFileSize(100 * 1024 * 1024L).build();

    UTIL.getAdmin().createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 5);
    UTIL.waitTableAvailable(tableName);

    try {
      // Step 2: Capture initial hashes
      List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
      assertEquals("Expected 5 regions", 5, regions.size());

      Map<byte[], String> initialHashes = new HashMap<>();

      for (HRegion region : regions) {
        String hash;
        try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
          hash = wrapper.getTableDescriptorHash();
        }
        initialHashes.put(region.getRegionInfo().getRegionName(), hash);
      }

      String initialHash = initialHashes.values().iterator().next();

      // Step 3: Perform lazy update
      ColumnFamilyDescriptor newCfd =
        ColumnFamilyDescriptorBuilder.newBuilder(cfd).setMaxVersions(10).build();

      TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td).modifyColumnFamily(newCfd)
        .setMaxFileSize(300 * 1024 * 1024L).build();

      UTIL.getAdmin().modifyTableAsync(newTd, false).get();

      UTIL.waitFor(30000, () -> {
        try {
          TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(tableName);
          return currentTd.getMaxFileSize() == 300 * 1024 * 1024L;
        } catch (Exception e) {
          return false;
        }
      });

      String newDescriptorHash = UTIL.getAdmin().getDescriptor(tableName).getDescriptorHash();

      // Step 4: Reopen only first 2 regions
      List<RegionInfo> regionsToReopen = new ArrayList<>();
      regionsToReopen.add(regions.get(0).getRegionInfo());
      regionsToReopen.add(regions.get(1).getRegionInfo());

      UTIL.getAdmin().reopenTableRegions(tableName, regionsToReopen);

      // Wait for those regions to reopen
      UTIL.waitFor(60000, () -> {
        try {
          List<HRegion> currentRegions = UTIL.getHBaseCluster().getRegions(tableName);
          int newHashCount = 0;
          for (HRegion region : currentRegions) {
            String hash;
            try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
              hash = wrapper.getTableDescriptorHash();
            }
            if (!hash.equals(initialHash)) {
              newHashCount++;
            }
          }
          return newHashCount >= 2;
        } catch (Exception e) {
          return false;
        }
      });

      // Step 5: Verify only reopened regions have new hash
      List<HRegion> regionsAfterFirstReopen = UTIL.getHBaseCluster().getRegions(tableName);
      int newHashCount = 0;
      int oldHashCount = 0;

      for (HRegion region : regionsAfterFirstReopen) {
        String currentHash;
        try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
          currentHash = wrapper.getTableDescriptorHash();
        }

        if (currentHash.equals(newDescriptorHash)) {
          newHashCount++;
        } else if (currentHash.equals(initialHash)) {
          oldHashCount++;
        }
      }

      assertEquals("Should have 2 regions with new hash", 2, newHashCount);
      assertEquals("Should have 3 regions with old hash", 3, oldHashCount);

      // Step 6: Reopen remaining regions
      List<RegionInfo> remainingRegions = new ArrayList<>();
      for (int i = 2; i < regions.size(); i++) {
        remainingRegions.add(regions.get(i).getRegionInfo());
      }

      UTIL.getAdmin().reopenTableRegions(tableName, remainingRegions);

      // Wait for all regions to have new hash
      UTIL.waitFor(60000, () -> {
        try {
          List<HRegion> currentRegions = UTIL.getHBaseCluster().getRegions(tableName);
          for (HRegion region : currentRegions) {
            String hash;
            try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
              hash = wrapper.getTableDescriptorHash();
            }
            if (!hash.equals(newDescriptorHash)) {
              return false;
            }
          }
          return true;
        } catch (Exception e) {
          return false;
        }
      });

      // Step 7: Verify all regions now have new hash
      List<HRegion> finalRegions = UTIL.getHBaseCluster().getRegions(tableName);
      for (HRegion region : finalRegions) {
        String currentHash;
        try (MetricsRegionWrapperImpl wrapper = new MetricsRegionWrapperImpl(region)) {
          currentHash = wrapper.getTableDescriptorHash();
        }

        assertEquals("All regions should now have new hash", newDescriptorHash, currentHash);
      }

    } finally {
      UTIL.deleteTable(tableName);
    }
  }
}
