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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.ipc.RpcServer.MAX_REQUEST_SIZE;
import static org.junit.Assert.assertThrows;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestRequestTooBigException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRequestTooBigException.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final TableName NAME = TableName.valueOf("request_too_big");

  private static final byte[] FAMILY = Bytes.toBytes("family");

  private static Table TABLE;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(MAX_REQUEST_SIZE, 10 * 1024);
    TEST_UTIL.startMiniCluster(1);
    TABLE = TEST_UTIL.createTable(NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(TABLE, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHbasePutDeleteCell() throws Exception {
    byte[] value = new byte[1024];
    Bytes.random(value);
    for (int m = 0; m < 100; m++) {
      Put p = new Put(Bytes.toBytes("bigrow-" + m));
      // max request is 10K, big request = 100 * 1K
      for (int i = 0; i < 100; i++) {
        p.addColumn(FAMILY, Bytes.toBytes("someQualifier" + i), value);
      }
      final Put finalPut = p;
      assertThrows(RequestTooBigException.class, () -> TABLE.put(finalPut));
    }
  }
}
