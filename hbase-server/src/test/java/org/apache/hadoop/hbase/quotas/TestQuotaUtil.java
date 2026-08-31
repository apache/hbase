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
package org.apache.hadoop.hbase.quotas;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestQuotaUtil {

  @Test
  public void testMetaTableIsThrottleExempt() {
    assertTrue(QuotaUtil.isThrottleExempt(TableName.META_TABLE_NAME));
  }

  @Test
  public void testQuotaTableIsThrottleExempt() {
    assertTrue(QuotaUtil.isThrottleExempt(QuotaTableUtil.QUOTA_TABLE_NAME));
  }

  @Test
  public void testBackupTablesAreNotThrottleExempt() {
    // Backup tables live in a system namespace, so they were previously exempt. They can
    // accumulate very large cells, which makes them a meaningful source of IO pressure, so
    // operators need to be able to throttle them.
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("backup:system")));
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("backup:system_bulk")));
  }

  @Test
  public void testOtherSystemTablesAreNotThrottleExempt() {
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("hbase:namespace")));
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("hbase:acl")));
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("hbase:labels")));
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("hbase:rsgroup")));
  }

  @Test
  public void testUserTablesAreNotThrottleExempt() {
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("my_table")));
    assertFalse(QuotaUtil.isThrottleExempt(TableName.valueOf("my_ns:my_table")));
  }
}
