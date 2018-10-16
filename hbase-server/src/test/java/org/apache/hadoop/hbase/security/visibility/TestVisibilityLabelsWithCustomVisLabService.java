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
package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_FAMILY;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SecurityTests.class, MediumTests.class})
public class TestVisibilityLabelsWithCustomVisLabService extends TestVisibilityLabels {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVisibilityLabelsWithCustomVisLabService.class);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    VisibilityTestUtil.enableVisiblityLabels(conf);
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
        ScanLabelGenerator.class);
    conf.setClass(VisibilityLabelServiceManager.VISIBILITY_LABEL_SERVICE_CLASS,
        ExpAsStringVisibilityLabelServiceImpl.class, VisibilityLabelService.class);
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(2);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    addLabels();
  }

  // Extending this test from super as we don't verify predefined labels in ExpAsStringVisibilityLabelServiceImpl
  @Override
  @Test
  public void testVisibilityLabelsInPutsThatDoesNotMatchAnyDefinedLabels() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    // This put with label "SAMPLE_LABEL" should not get failed.
    createTableAndWriteDataWithLabels(tableName, "SAMPLE_LABEL", "TEST");
  }

  @Override
  protected List<String> extractAuths(String user, List<Result> results) {
    List<String> auths = new ArrayList<>();
    for (Result result : results) {
      if (Bytes.equals(result.getRow(), Bytes.toBytes(user))) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(LABELS_TABLE_FAMILY);
        for (byte[] q : familyMap.keySet()) {
          auths.add(Bytes.toString(q, 0, q.length));
        }
      }
    }
    return auths;
  }
}
