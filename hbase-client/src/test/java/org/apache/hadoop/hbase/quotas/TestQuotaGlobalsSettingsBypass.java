/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory.QuotaGlobalsSettingsBypass;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestQuotaGlobalsSettingsBypass {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQuotaGlobalsSettingsBypass.class);

  @Test
  public void testMerge() throws IOException {
    QuotaGlobalsSettingsBypass orig = new QuotaGlobalsSettingsBypass("joe", null, null, true);
    assertFalse(orig.merge(new QuotaGlobalsSettingsBypass(
        "joe", null, null, false)).getBypass());
  }

  @Test
  public void testInvalidMerges() throws IOException {
    QuotaGlobalsSettingsBypass userBypass = new QuotaGlobalsSettingsBypass(
        "joe", null, null, true);
    QuotaGlobalsSettingsBypass tableBypass = new QuotaGlobalsSettingsBypass(
        null, TableName.valueOf("table"), null, true);
    QuotaGlobalsSettingsBypass namespaceBypass = new QuotaGlobalsSettingsBypass(
        null, null, "ns", true);
    QuotaGlobalsSettingsBypass userOnTableBypass = new QuotaGlobalsSettingsBypass(
        "joe", TableName.valueOf("table"), null, true);
    QuotaGlobalsSettingsBypass userOnNamespaceBypass = new QuotaGlobalsSettingsBypass(
        "joe", null, "ns", true);

    assertTrue(userBypass.merge(userBypass).getBypass());
    expectFailure(userBypass, new QuotaGlobalsSettingsBypass("frank", null, null, false));
    expectFailure(userBypass, tableBypass);
    expectFailure(userBypass, namespaceBypass);
    expectFailure(userBypass, userOnTableBypass);
    expectFailure(userBypass, userOnNamespaceBypass);

    assertTrue(tableBypass.merge(tableBypass).getBypass());
    expectFailure(tableBypass, userBypass);
    expectFailure(tableBypass, new QuotaGlobalsSettingsBypass(
        null, TableName.valueOf("foo"), null, false));
    expectFailure(tableBypass, namespaceBypass);
    expectFailure(tableBypass, userOnTableBypass);
    expectFailure(tableBypass, userOnNamespaceBypass);

    assertTrue(namespaceBypass.merge(namespaceBypass).getBypass());
    expectFailure(namespaceBypass, userBypass);
    expectFailure(namespaceBypass, tableBypass);
    expectFailure(namespaceBypass, new QuotaGlobalsSettingsBypass(null, null, "sn", false));
    expectFailure(namespaceBypass, userOnTableBypass);
    expectFailure(namespaceBypass, userOnNamespaceBypass);

    assertTrue(userOnTableBypass.merge(userOnTableBypass).getBypass());
    expectFailure(userOnTableBypass, userBypass);
    expectFailure(userOnTableBypass, tableBypass);
    expectFailure(userOnTableBypass, namespaceBypass);
    // Incorrect user
    expectFailure(userOnTableBypass, new QuotaGlobalsSettingsBypass(
        "frank", TableName.valueOf("foo"), null, false));
    // Incorrect tablename
    expectFailure(userOnTableBypass, new QuotaGlobalsSettingsBypass(
        "joe", TableName.valueOf("bar"), null, false));
    expectFailure(userOnTableBypass, userOnNamespaceBypass);

    assertTrue(userOnNamespaceBypass.merge(userOnNamespaceBypass).getBypass());
    expectFailure(userOnNamespaceBypass, userBypass);
    expectFailure(userOnNamespaceBypass, tableBypass);
    expectFailure(userOnNamespaceBypass, namespaceBypass);
    expectFailure(userOnNamespaceBypass, userOnTableBypass);
    expectFailure(userOnNamespaceBypass, new QuotaGlobalsSettingsBypass(
        "frank", null, "ns", false));
    expectFailure(userOnNamespaceBypass, new QuotaGlobalsSettingsBypass(
        "joe", null, "sn", false));
  }

  void expectFailure(QuotaSettings one, QuotaSettings two) throws IOException {
    try {
      one.merge(two);
      fail("Expected to see an Exception merging " + two + " into " + one);
    } catch (IllegalArgumentException e) {}
  }
}
