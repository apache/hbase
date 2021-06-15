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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class, ClientTests.class})
public class TestRequestTooBigException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRequestTooBigException.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHbasePutDeleteCell() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] family = Bytes.toBytes("cf");
    Table table = TEST_UTIL.createTable(tableName, family);
    TEST_UTIL.waitTableAvailable(tableName.getName(), 5000);
    try {
      byte[] value = new byte[2 * 2014 * 1024];

      Put p = new Put(Bytes.toBytes("bigrow"));
      // big request = 400*2 M
      for (int i = 0; i < 400; i++) {
        p.addColumn(family, Bytes.toBytes("someQualifier" + i), value);
      }
      try {
        table.put(p);
        assertTrue("expected RequestTooBigException", false);
      } catch (RequestTooBigException e) {
        assertTrue("expected RequestTooBigException", true);
      }
    } finally {
      table.close();
    }
  }
}


