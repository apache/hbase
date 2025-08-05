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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestRecoverySnapshotUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRecoverySnapshotUtils.class);

  @Test
  public void testRecoverySnapshotTtlNoDescriptor() {
    // Create a mock MasterProcedureEnv with a known site configuration TTL
    long siteLevelTtl = 7200; // 2 hours
    Configuration conf = new Configuration();
    conf.setLong(HConstants.SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_TTL_KEY, siteLevelTtl);

    MasterProcedureEnv env = mock(MasterProcedureEnv.class);
    when(env.getMasterConfiguration()).thenReturn(conf);

    // Test with null table descriptor - should return site configuration
    long actualTtl = RecoverySnapshotUtils.getRecoverySnapshotTtl(env, null);
    assertEquals("Should return site-level TTL when no table descriptor provided", siteLevelTtl,
      actualTtl);
  }

  @Test
  public void testRecoverySnapshotTtlWithDescriptor() {
    // Create a mock MasterProcedureEnv with a known site configuration TTL
    long siteLevelTtl = 7200; // 2 hours
    Configuration conf = new Configuration();
    conf.setLong(HConstants.SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_TTL_KEY, siteLevelTtl);

    MasterProcedureEnv env = mock(MasterProcedureEnv.class);
    when(env.getMasterConfiguration()).thenReturn(conf);

    // Create a table descriptor with a different TTL override
    long tableLevelTtl = 3600; // 1 hour
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("test"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
      .setValue(HConstants.TABLE_RECOVERY_SNAPSHOT_TTL_KEY, String.valueOf(tableLevelTtl)).build();

    // Test with table descriptor override - should return table-level TTL
    long actualTtl = RecoverySnapshotUtils.getRecoverySnapshotTtl(env, tableDescriptor);
    assertEquals("Should return table-level TTL when table descriptor provides override",
      tableLevelTtl, actualTtl);
  }

  @Test
  public void testRecoverySnapshotTtlUsesDefault() {
    // Create a mock MasterProcedureEnv with default configuration (no explicit TTL set)
    Configuration conf = new Configuration();
    // Don't set the TTL key, so it should use the default

    MasterProcedureEnv env = mock(MasterProcedureEnv.class);
    when(env.getMasterConfiguration()).thenReturn(conf);

    // Test with null table descriptor - should return default TTL
    long actualTtl = RecoverySnapshotUtils.getRecoverySnapshotTtl(env, null);
    assertEquals("Should return default TTL when no site configuration provided",
      HConstants.DEFAULT_SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_TTL, actualTtl);
  }
}
