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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_REGIONSERVER_PORT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({MiscTests.class, SmallTests.class})
public class TestRegionSizeCalculator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionSizeCalculator.class);

  private Configuration configuration = new Configuration();
  private final long megabyte = 1024L * 1024L;
  private final ServerName sn = ServerName.valueOf("local-rs", DEFAULT_REGIONSERVER_PORT,
      ServerName.NON_STARTCODE);

  @Test
  public void testSimpleTestCase() throws Exception {

    RegionLocator regionLocator = mockRegionLocator("region1", "region2", "region3");

    Admin admin = mockAdmin(
        mockRegion("region1", 123),
        mockRegion("region3", 1232),
        mockRegion("region2",  54321)
    );

    RegionSizeCalculator calculator = new RegionSizeCalculator(regionLocator, admin);

    assertEquals(123 * megabyte, calculator.getRegionSize("region1".getBytes()));
    assertEquals(54321 * megabyte, calculator.getRegionSize("region2".getBytes()));
    assertEquals(1232 * megabyte, calculator.getRegionSize("region3".getBytes()));
    // if regionCalculator does not know about a region, it should return 0
    assertEquals(0 * megabyte, calculator.getRegionSize("otherTableRegion".getBytes()));

    assertEquals(3, calculator.getRegionSizeMap().size());
  }


  /**
   * When size of region in megabytes is larger than largest possible integer there could be
   * error caused by lost of precision.
   * */
  @Test
  public void testLargeRegion() throws Exception {

    RegionLocator regionLocator = mockRegionLocator("largeRegion");

    Admin admin = mockAdmin(
        mockRegion("largeRegion", Integer.MAX_VALUE)
    );

    RegionSizeCalculator calculator = new RegionSizeCalculator(regionLocator, admin);

    assertEquals(((long) Integer.MAX_VALUE) * megabyte, calculator.getRegionSize("largeRegion".getBytes()));
  }

  /** When calculator is disabled, it should return 0 for each request.*/
  @Test
  public void testDisabled() throws Exception {
    String regionName = "cz.goout:/index.html";
    RegionLocator table = mockRegionLocator(regionName);

    Admin admin = mockAdmin(
        mockRegion(regionName, 999)
    );

    //first request on enabled calculator
    RegionSizeCalculator calculator = new RegionSizeCalculator(table, admin);
    assertEquals(999 * megabyte, calculator.getRegionSize(regionName.getBytes()));

    //then disabled calculator.
    configuration.setBoolean(RegionSizeCalculator.ENABLE_REGIONSIZECALCULATOR, false);
    RegionSizeCalculator disabledCalculator = new RegionSizeCalculator(table, admin);
    assertEquals(0 * megabyte, disabledCalculator.getRegionSize(regionName.getBytes()));

    assertEquals(0, disabledCalculator.getRegionSizeMap().size());
  }

  /**
   * Makes some table with given region names.
   * */
  private RegionLocator mockRegionLocator(String... regionNames) throws IOException {
    RegionLocator mockedTable = Mockito.mock(RegionLocator.class);
    when(mockedTable.getName()).thenReturn(TableName.valueOf("sizeTestTable"));
    List<HRegionLocation> regionLocations = new ArrayList<>(regionNames.length);
    when(mockedTable.getAllRegionLocations()).thenReturn(regionLocations);

    for (String regionName : regionNames) {
      HRegionInfo info = Mockito.mock(HRegionInfo.class);
      when(info.getRegionName()).thenReturn(regionName.getBytes());
      regionLocations.add(new HRegionLocation(info, sn));
    }

    return mockedTable;
  }

  /**
   * Creates mock returning RegionLoad info about given servers.
  */
  private Admin mockAdmin(RegionMetrics... regionLoadArray) throws Exception {
    Admin mockAdmin = Mockito.mock(Admin.class);
    List<RegionMetrics> regionLoads = new ArrayList<>();
    for (RegionMetrics regionLoad : regionLoadArray) {
      regionLoads.add(regionLoad);
    }
    when(mockAdmin.getConfiguration()).thenReturn(configuration);
    when(mockAdmin.getRegionMetrics(sn, TableName.valueOf("sizeTestTable")))
        .thenReturn(regionLoads);
    return mockAdmin;
  }

  /**
   * Creates mock of region with given name and size.
   *
   * @param  fileSizeMb number of megabytes occupied by region in file store in megabytes
   * */
  private RegionMetrics mockRegion(String regionName, int fileSizeMb) {
    RegionMetrics region = Mockito.mock(RegionMetrics.class);
    when(region.getRegionName()).thenReturn(regionName.getBytes());
    when(region.getNameAsString()).thenReturn(regionName);
    when(region.getStoreFileSize()).thenReturn(new Size(fileSizeMb, Size.Unit.MEGABYTE));
    return region;
  }
}
