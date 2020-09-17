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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestCatalogFamilyFormat {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCatalogFamilyFormat.class);

  @Test
  public void testParseReplicaIdFromServerColumn() {
    String column1 = HConstants.SERVER_QUALIFIER_STR;
    assertEquals(0, CatalogFamilyFormat.parseReplicaIdFromServerColumn(Bytes.toBytes(column1)));
    String column2 = column1 + CatalogFamilyFormat.META_REPLICA_ID_DELIMITER;
    assertEquals(-1, CatalogFamilyFormat.parseReplicaIdFromServerColumn(Bytes.toBytes(column2)));
    String column3 = column2 + "00";
    assertEquals(-1, CatalogFamilyFormat.parseReplicaIdFromServerColumn(Bytes.toBytes(column3)));
    String column4 = column3 + "2A";
    assertEquals(42, CatalogFamilyFormat.parseReplicaIdFromServerColumn(Bytes.toBytes(column4)));
    String column5 = column4 + "2A";
    assertEquals(-1, CatalogFamilyFormat.parseReplicaIdFromServerColumn(Bytes.toBytes(column5)));
    String column6 = HConstants.STARTCODE_QUALIFIER_STR;
    assertEquals(-1, CatalogFamilyFormat.parseReplicaIdFromServerColumn(Bytes.toBytes(column6)));
  }

  @Test
  public void testMetaReaderGetColumnMethods() {
    assertArrayEquals(HConstants.SERVER_QUALIFIER, CatalogFamilyFormat.getServerColumn(0));
    assertArrayEquals(
      Bytes.toBytes(
        HConstants.SERVER_QUALIFIER_STR + CatalogFamilyFormat.META_REPLICA_ID_DELIMITER + "002A"),
      CatalogFamilyFormat.getServerColumn(42));

    assertArrayEquals(HConstants.STARTCODE_QUALIFIER, CatalogFamilyFormat.getStartCodeColumn(0));
    assertArrayEquals(
      Bytes.toBytes(HConstants.STARTCODE_QUALIFIER_STR +
        CatalogFamilyFormat.META_REPLICA_ID_DELIMITER + "002A"),
      CatalogFamilyFormat.getStartCodeColumn(42));

    assertArrayEquals(HConstants.SEQNUM_QUALIFIER, CatalogFamilyFormat.getSeqNumColumn(0));
    assertArrayEquals(
      Bytes.toBytes(
        HConstants.SEQNUM_QUALIFIER_STR + CatalogFamilyFormat.META_REPLICA_ID_DELIMITER + "002A"),
      CatalogFamilyFormat.getSeqNumColumn(42));
  }
}
