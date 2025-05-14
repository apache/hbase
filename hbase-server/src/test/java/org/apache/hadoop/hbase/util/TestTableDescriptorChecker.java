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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.conf.ConfigKey;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestTableDescriptorChecker {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableDescriptorChecker.class);

  @Test
  public void testSanityCheck() throws IOException {
    Configuration conf = new Configuration();
    TableDescriptorBuilder t = TableDescriptorBuilder.newBuilder(TableName.valueOf("test"));
    ColumnFamilyDescriptorBuilder cf = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
    t.setColumnFamily(cf.build());

    // Empty configuration. Should be fine.
    TableDescriptorChecker.sanityCheck(conf, t.build());

    // Declare configuration type as int.
    String key = "hbase.hstore.compaction.ratio";
    ConfigKey.INT(key, v -> v > 0);

    // Error in table configuration.
    t.setValue(key, "xx");
    assertThrows("Should have thrown IllegalArgumentException", DoNotRetryIOException.class,
      () -> TableDescriptorChecker.sanityCheck(conf, t.build()));

    // Fix the error.
    t.setValue(key, "1");
    TableDescriptorChecker.sanityCheck(conf, t.build());

    // Verify column family configuration.
    for (boolean viaSetValue : new boolean[] { true, false }) {
      // Error in column family configuration.
      if (viaSetValue) {
        cf.setValue(key, "xx");
      } else {
        cf.setConfiguration(key, "xx");
      }
      t.removeColumnFamily("cf".getBytes());
      t.setColumnFamily(cf.build());
      assertThrows("Should have thrown IllegalArgumentException", DoNotRetryIOException.class,
        () -> TableDescriptorChecker.sanityCheck(conf, t.build()));

      // Fix the error.
      if (viaSetValue) {
        cf.setValue(key, "");
      } else {
        cf.setConfiguration(key, "");
      }
      t.removeColumnFamily("cf".getBytes());
      t.setColumnFamily(cf.build());
      TableDescriptorChecker.sanityCheck(conf, t.build());
    }
  }
}
