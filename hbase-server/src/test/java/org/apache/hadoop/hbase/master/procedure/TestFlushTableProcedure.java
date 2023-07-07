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

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestFlushTableProcedure extends TestFlushTableProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFlushTableProcedure.class);

  @Test
  public void testSimpleFlush() throws IOException {
    assertTableMemStoreNotEmpty();
    TEST_UTIL.getAdmin().flush(TABLE_NAME);
    assertTableMemStoreEmpty();
  }

  @Test
  public void testFlushTableExceptionally() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    admin.disableTable(TABLE_NAME);
    Assert.assertThrows(TableNotEnabledException.class, () -> admin.flush(TABLE_NAME));
    admin.deleteTable(TABLE_NAME);
    Assert.assertThrows(TableNotFoundException.class, () -> admin.flush(TABLE_NAME));
  }

  @Test
  public void testSingleColumnFamilyFlush() throws IOException {
    assertTableMemStoreNotEmpty();
    TEST_UTIL.getAdmin().flush(TABLE_NAME, Arrays.asList(FAMILY1, FAMILY2, FAMILY3));
    assertTableMemStoreEmpty();
  }

  @Test
  public void testMultiColumnFamilyFlush() throws IOException {
    assertTableMemStoreNotEmpty();
    TEST_UTIL.getAdmin().flush(TABLE_NAME, Arrays.asList(FAMILY1, FAMILY2, FAMILY3));
    assertTableMemStoreEmpty();
  }
}
