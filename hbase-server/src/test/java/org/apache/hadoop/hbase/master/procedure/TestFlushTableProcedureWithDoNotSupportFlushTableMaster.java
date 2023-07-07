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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestFlushTableProcedureWithDoNotSupportFlushTableMaster
  extends TestFlushTableProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFlushTableProcedureWithDoNotSupportFlushTableMaster.class);

  @Override
  protected void addConfiguration(Configuration config) {
    super.addConfiguration(config);
    config.set(HConstants.MASTER_IMPL, DoNotSupportFlushTableMaster.class.getName());
  }

  @Test
  public void testFlushFallback() throws IOException {
    assertTableMemStoreNotEmpty();
    TEST_UTIL.getAdmin().flush(TABLE_NAME);
    assertTableMemStoreEmpty();
  }

  @Test
  public void testSingleColumnFamilyFlushFallback() throws IOException {
    assertColumnFamilyMemStoreNotEmpty(FAMILY1);
    TEST_UTIL.getAdmin().flush(TABLE_NAME, FAMILY1);
    assertColumnFamilyMemStoreEmpty(FAMILY1);
  }

  @Test
  public void testMultiColumnFamilyFlushFallback() throws IOException {
    assertTableMemStoreNotEmpty();
    TEST_UTIL.getAdmin().flush(TABLE_NAME, Arrays.asList(FAMILY1, FAMILY2, FAMILY3));
    assertTableMemStoreEmpty();
  }

  public static final class DoNotSupportFlushTableMaster extends HMaster {

    public DoNotSupportFlushTableMaster(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public long flushTable(TableName tableName, List<byte[]> columnFamilies, long nonceGroup,
      long nonce) throws IOException {
      throw new DoNotRetryIOException("UnsupportedOperation: flushTable");
    }
  }
}
