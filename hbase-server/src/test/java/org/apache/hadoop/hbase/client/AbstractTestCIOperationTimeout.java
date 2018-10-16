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
import java.net.SocketTimeoutException;
import org.apache.hadoop.hbase.TableName;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Based class for testing operation timeout logic for {@link ConnectionImplementation}.
 */
public abstract class AbstractTestCIOperationTimeout extends AbstractTestCITimeout {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestCIOperationTimeout.class);

  private TableName tableName;

  @Before
  public void setUp() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setCoprocessor(SleepAndFailFirstTime.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAM_NAM)).build();
    TEST_UTIL.getAdmin().createTable(htd);
  }

  protected abstract void execute(Table table) throws IOException;

  /**
   * Test that an operation can fail if we read the global operation timeout, even if the individual
   * timeout is fine. We do that with:
   * <ul>
   * <li>client side: an operation timeout of 30 seconds</li>
   * <li>server side: we sleep 20 second at each attempt. The first work fails, the second one
   * succeeds. But the client won't wait that much, because 20 + 20 > 30, so the client timed out
   * when the server answers.</li>
   * </ul>
   */
  @Test
  public void testOperationTimeout() throws IOException {
    TableBuilder builder =
      TEST_UTIL.getConnection().getTableBuilder(tableName, null).setRpcTimeout(Integer.MAX_VALUE)
          .setReadRpcTimeout(Integer.MAX_VALUE).setWriteRpcTimeout(Integer.MAX_VALUE);
    // Check that it works if the timeout is big enough
    SleepAndFailFirstTime.ct.set(0);
    try (Table table = builder.setOperationTimeout(120 * 1000).build()) {
      execute(table);
    }
    // Resetting and retrying. Will fail this time, not enough time for the second try
    SleepAndFailFirstTime.ct.set(0);
    try (Table table = builder.setOperationTimeout(30 * 1000).build()) {
      SleepAndFailFirstTime.ct.set(0);
      execute(table);
      fail("We expect an exception here");
    } catch (SocketTimeoutException | RetriesExhaustedWithDetailsException e) {
      // The client has a CallTimeout class, but it's not shared. We're not very clean today,
      // in the general case you can expect the call to stop, but the exception may vary.
      // In this test however, we're sure that it will be a socket timeout.
      LOG.info("We received an exception, as expected ", e);
    }
  }
}
