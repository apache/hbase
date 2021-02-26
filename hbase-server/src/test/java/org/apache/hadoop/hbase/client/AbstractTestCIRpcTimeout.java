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

import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Based class for testing rpc timeout logic for {@link ConnectionImplementation}.
 */
public abstract class AbstractTestCIRpcTimeout extends AbstractTestCITimeout {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestCIRpcTimeout.class);

  private TableName tableName;

  @Before
  public void setUp() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(tableName).setCoprocessor(SleepCoprocessor.class.getName())
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAM_NAM)).build();
    TEST_UTIL.getAdmin().createTable(htd);
  }

  protected abstract void execute(Table table) throws IOException;

  @Test
  public void testRpcTimeout() throws IOException {
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    try (Table table = TEST_UTIL.getConnection().getTableBuilder(tableName, null)
        .setRpcTimeout(SleepCoprocessor.SLEEP_TIME / 2)
        .setReadRpcTimeout(SleepCoprocessor.SLEEP_TIME / 2)
        .setWriteRpcTimeout(SleepCoprocessor.SLEEP_TIME / 2)
        .setOperationTimeout(SleepCoprocessor.SLEEP_TIME * 100).build()) {
      execute(table);
      fail("Get should not have succeeded");
    } catch (RetriesExhaustedException e) {
      LOG.info("We received an exception, as expected ", e);
    }

    // Again, with configuration based override
    c.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, SleepCoprocessor.SLEEP_TIME / 2);
    c.setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, SleepCoprocessor.SLEEP_TIME / 2);
    c.setInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, SleepCoprocessor.SLEEP_TIME / 2);
    c.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, SleepCoprocessor.SLEEP_TIME * 100);
    try (Connection conn = ConnectionFactory.createConnection(c)) {
      try (Table table = conn.getTable(tableName)) {
        execute(table);
        fail("Get should not have succeeded");
      } catch (RetriesExhaustedException e) {
        LOG.info("We received an exception, as expected ", e);
      }
    }
  }
}
