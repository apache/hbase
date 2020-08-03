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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.hamcrest.Matchers;
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
  protected static String REGION_NAME = UUID.randomUUID().toString().replaceAll("-", "");
  protected static String STORE_NAME = UUID.randomUUID().toString();
  protected static List<Path> EMPTY_PATH = Collections.emptyList();
  protected static List<Path> INCLUDE_EXAMPLE_PATH =
    Lists.newArrayList(new Path("hdfs://foo/bar1"), new Path("hdfs://foo/bar2"));
  protected static final String VALID_TABLE_NAME_CHARS = "_.";

  protected String tableName;

  protected abstract StoreFilePathAccessor getStoreFilePathAccessor() throws IOException;

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
    tableName = name.getMethodName() + VALID_TABLE_NAME_CHARS + UUID.randomUUID();
    init();
    storeFilePathAccessor = getStoreFilePathAccessor();
  }

  abstract void init() throws Exception;

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

  abstract void verifyNotInitializedException();

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
  public void testWriteIncludedStoreFilePathsWhenNotInitialized() throws Exception {
    verifyNotInitializedException();
    writeAndVerifyIncludedFilePaths(INCLUDE_EXAMPLE_PATH);
  }

  @Test
  public void testGetIncludedStoreFilePathsWhenNotInitialized() throws Exception {
    verifyNotInitializedException();
    storeFilePathAccessor.getIncludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
  }

  @Test
  public void testWriteIncludedStoreFilePathsWithEmptyList() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    testInitialize();
    // verify empty before write
    verifyIncludedFilePaths(EMPTY_PATH);
    // write and verify empty list fails
    writeAndVerifyIncludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testWriteIncludedStoreFilePaths() throws Exception {
    testInitialize();
    verifyIncludedFilePaths(EMPTY_PATH);
    writeAndVerifyIncludedFilePaths(INCLUDE_EXAMPLE_PATH);
  }

  @Test
  public void testWriteIncludedStoreFilePathsWithNull() throws Exception {
    expectedException.expect(NullPointerException.class);
    testInitialize();
    // verify empty before write
    verifyIncludedFilePaths(EMPTY_PATH);
    // write and verify empty list fails
    writeAndVerifyIncludedFilePaths(null);
  }

  @Test
  public void testDeleteStoreFilePaths() throws Exception {
    testInitialize();

    // verify empty list before write
    verifyIncludedFilePaths(EMPTY_PATH);
    // write some date to included:files data set
    writeAndVerifyIncludedFilePaths(INCLUDE_EXAMPLE_PATH);
    // delete and verify both data set are empty
    storeFilePathAccessor.deleteStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
    verifyIncludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testDeleteStoreFilePathsWithNoData() throws Exception {
    testInitialize();

    // verify empty list before write
    verifyIncludedFilePaths(EMPTY_PATH);
    // delete and verify both data set are empty
    storeFilePathAccessor.deleteStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
    verifyIncludedFilePaths(EMPTY_PATH);
  }

  @Test
  public void testDeleteStoreFilePathsWhenNotInitialized() throws Exception {
    expectedException.expectCause(Matchers.isA(TableNotFoundException.class));
    storeFilePathAccessor.deleteStoreFilePaths(tableName, REGION_NAME, STORE_NAME);
  }

  protected void writeAndVerifyIncludedFilePaths(List<Path> paths) throws IOException {
    storeFilePathAccessor.writeStoreFilePaths(tableName, REGION_NAME, STORE_NAME,
      StoreFilePathUpdate.builder().withStorePaths(paths).build());
    verifyIncludedFilePaths(paths);
  }

  protected void verifyIncludedFilePaths(List<Path> expectPaths) throws IOException {
    assertEquals(expectPaths, storeFilePathAccessor
      .getIncludedStoreFilePaths(tableName, REGION_NAME, STORE_NAME));
  }
}
