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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.master.MasterServices;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

public abstract class StoreFilePathAccessorTestBase {

  @Rule
  public TestName name = new TestName();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected StoreFilePathAccessor storeFilePathAccessor;
  protected static String REGION_NAME = "r1";
  protected static String STORE_NAME = "cf1";
  protected static List<Path> EMPTY_PATH = Collections.emptyList();
  protected static List<Path> INCLUDE_EXAMPLE_PATH =
      Lists.newArrayList(new Path("hdfs://foo/bar1"), new Path("hdfs://foo/bar2"));
  protected static List<Path> EXCLUDE_EXAMPLE_PATH =
      Lists.newArrayList(new Path("hdfs://foo/bar3"), new Path("hdfs://foo/bar4"));

  protected String tableName;

  protected abstract StoreFilePathAccessor getStoreFilePathAccessor();

  @BeforeClass
  public static void setUpCluster() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    storeFilePathAccessor = getStoreFilePathAccessor();
    tableName = name.getMethodName();
  }

  @After
  public void after() throws Exception {
    cleanupTest();
  }

  abstract void cleanupTest() throws Exception;

  @Test
  public void testInitialize() throws Exception {
    MasterServices masterServices = TEST_UTIL.getHBaseCluster().getMaster();
    verifyInitialize(masterServices);
  }

  // this will be implemented by each implementation of StoreFilePathAccessor
  abstract void verifyInitialize(MasterServices masterServices) throws Exception;

  abstract Class getNotInitializedException();

  @Test
  public void testIncludedStoreFilePaths() throws Exception {
    testInitialize();
    // verify empty list before write
    verifyIncludedFilePaths(EMPTY_PATH);
    writeAndVerifyIncludedFilePaths(INCLUDE_EXAMPLE_PATH);
  }

  @Test
  public void testIncludedStoreFilePathsWithEmptyList() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    testInitialize();
    // verify empty before write
    verifyIncludedFilePaths(EMPTY_PATH);
    // write and verify empty list fails
    writeAndVerifyIncludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testIncludedStoreFilePathsWhenNotInitialized() throws Exception {
    expectedException.expect(getNotInitializedException());
    writeAndVerifyIncludedFilePaths(INCLUDE_EXAMPLE_PATH);
  }

  @Test
  public void testGetIncludedStoreFilePathsWhenNotInitialized() throws Exception {
    expectedException.expect(getNotInitializedException());
    storeFilePathAccessor.getIncludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
  }

  @Test
  public void testExcludedStoreFilePaths() throws Exception {
    testInitialize();
    // verify empty list before write
    verifyExcludedFilePaths(EMPTY_PATH);
    writeAndVerifyExcludedFilePaths(EXCLUDE_EXAMPLE_PATH);
    // write and verify empty list
    writeAndVerifyExcludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testExcludedStoreFilePathsWhenNotInitialized() throws Exception {
    expectedException.expect(getNotInitializedException());
    writeAndVerifyExcludedFilePaths(EXCLUDE_EXAMPLE_PATH);
  }

  @Test
  public void testGetExcludedStoreFilePathsWhenNotInitialized() throws Exception {
    expectedException.expect(getNotInitializedException());
    storeFilePathAccessor.getExcludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
  }

  @Test
  public void testDeleteStoreFilePaths() throws Exception {
    testInitialize();

    // verify empty list before write
    verifyIncludedFilePaths(EMPTY_PATH);
    verifyExcludedFilePaths(EMPTY_PATH);
    // write some date to both included and excluded data set
    writeAndVerifyIncludedFilePaths(INCLUDE_EXAMPLE_PATH);
    writeAndVerifyExcludedFilePaths(EXCLUDE_EXAMPLE_PATH);
    // delete and verify both data set are empty
    storeFilePathAccessor.deleteStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
    verifyIncludedFilePaths(EMPTY_PATH);
    verifyExcludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testDeleteStoreFilePathsWithEmptyList() throws Exception {
    testInitialize();

    // verify empty list before write
    verifyIncludedFilePaths(EMPTY_PATH);
    verifyExcludedFilePaths(EMPTY_PATH);
    // write empty list to excluded
    writeAndVerifyExcludedFilePaths(EMPTY_PATH);
    // delete and verify both data set are empty
    storeFilePathAccessor.deleteStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
    verifyIncludedFilePaths(EMPTY_PATH);
    verifyExcludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testDeleteStoreFilePathsWithNoData() throws Exception {
    testInitialize();

    // verify empty list before write
    verifyIncludedFilePaths(EMPTY_PATH);
    verifyExcludedFilePaths(EMPTY_PATH);
    // delete and verify both data set are empty
    storeFilePathAccessor.deleteStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
    verifyIncludedFilePaths(EMPTY_PATH);
    verifyExcludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testDeleteStoreFilePathsWhenNotInitialized() throws Exception {
    expectedException.expect(getNotInitializedException());
    writeAndVerifyExcludedFilePaths(EXCLUDE_EXAMPLE_PATH);
  }

  protected void writeAndVerifyIncludedFilePaths(List<Path> paths) throws IOException {
    storeFilePathAccessor.writeIncludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME,
        paths);
    verifyIncludedFilePaths(paths);
  }

  protected void writeAndVerifyExcludedFilePaths(List<Path> paths) throws IOException {
    storeFilePathAccessor.writeExcludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME,
        paths);
    verifyExcludedFilePaths(paths);
  }

  protected void verifyIncludedFilePaths(List<Path> expectPaths) throws IOException {
    assertEquals(expectPaths, storeFilePathAccessor
        .getIncludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME));
  }

  protected void verifyExcludedFilePaths(List<Path> expectPaths) throws IOException {
    assertEquals(expectPaths, storeFilePathAccessor
        .getExcludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME));
  }
}
