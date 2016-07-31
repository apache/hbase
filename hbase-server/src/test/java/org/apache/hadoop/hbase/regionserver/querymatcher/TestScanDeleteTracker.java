/*
 *
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

package org.apache.hadoop.hbase.regionserver.querymatcher;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestScanDeleteTracker {

  private ScanDeleteTracker sdt;

  private long timestamp = 10L;

  @Before
  public void setUp() throws Exception {
    sdt = new ScanDeleteTracker();
  }

  @Test
  public void testDeletedByDelete() {
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), Bytes.toBytes("qualifier"),
        timestamp, KeyValue.Type.Delete);
    sdt.add(kv);
    DeleteResult ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.VERSION_DELETED, ret);
  }

  @Test
  public void testDeletedByDeleteColumn() {
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), Bytes.toBytes("qualifier"),
        timestamp, KeyValue.Type.DeleteColumn);
    sdt.add(kv);
    timestamp -= 5;
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), Bytes.toBytes("qualifier"),
        timestamp, KeyValue.Type.DeleteColumn);
    DeleteResult ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.COLUMN_DELETED, ret);
  }

  @Test
  public void testDeletedByDeleteFamily() {
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), Bytes.toBytes("qualifier"),
        timestamp, KeyValue.Type.DeleteFamily);
    sdt.add(kv);
    timestamp -= 5;
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), Bytes.toBytes("qualifier"),
        timestamp, KeyValue.Type.DeleteColumn);
    DeleteResult ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.FAMILY_DELETED, ret);
  }

  @Test
  public void testDeletedByDeleteFamilyVersion() {
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    byte[] qualifier3 = Bytes.toBytes("qualifier3");
    byte[] qualifier4 = Bytes.toBytes("qualifier4");
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), null, timestamp,
        KeyValue.Type.DeleteFamilyVersion);
    sdt.add(kv);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier1, timestamp,
        KeyValue.Type.DeleteFamilyVersion);
    DeleteResult ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.FAMILY_VERSION_DELETED, ret);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier2, timestamp,
        KeyValue.Type.DeleteFamilyVersion);
    ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.FAMILY_VERSION_DELETED, ret);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier3, timestamp,
        KeyValue.Type.DeleteFamilyVersion);
    ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.FAMILY_VERSION_DELETED, ret);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier4, timestamp,
        KeyValue.Type.DeleteFamilyVersion);
    ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.FAMILY_VERSION_DELETED, ret);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier1, timestamp + 3,
        KeyValue.Type.DeleteFamilyVersion);
    ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.NOT_DELETED, ret);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier2, timestamp - 2,
        KeyValue.Type.DeleteFamilyVersion);
    ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.NOT_DELETED, ret);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier3, timestamp - 5,
        KeyValue.Type.DeleteFamilyVersion);
    ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.NOT_DELETED, ret);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier4, timestamp + 8,
        KeyValue.Type.DeleteFamilyVersion);
    ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.NOT_DELETED, ret);
  }

  @Test
  public void testDeleteDeleteColumn() {
    byte[] qualifier = Bytes.toBytes("qualifier");
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, timestamp,
        KeyValue.Type.Delete);
    sdt.add(kv);

    timestamp -= 5;
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, timestamp,
        KeyValue.Type.DeleteColumn);
    sdt.add(kv);

    timestamp -= 5;
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, timestamp,
        KeyValue.Type.DeleteColumn);
    DeleteResult ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.COLUMN_DELETED, ret);
  }

  @Test
  public void testDeleteColumnDelete() {
    byte[] qualifier = Bytes.toBytes("qualifier");
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, timestamp,
        KeyValue.Type.DeleteColumn);
    sdt.add(kv);

    qualifier = Bytes.toBytes("qualifier1");
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, timestamp,
        KeyValue.Type.Delete);
    sdt.add(kv);

    DeleteResult ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.VERSION_DELETED, ret);
  }

  // Testing new way where we save the Delete in case of a Delete for specific
  // ts, could have just added the last line to the first test, but rather keep
  // them separated
  @Test
  public void testDeleteKeepDelete() {
    byte[] qualifier = Bytes.toBytes("qualifier");
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, timestamp,
        KeyValue.Type.Delete);
    sdt.add(kv);
    sdt.isDeleted(kv);
    assertEquals(false, sdt.isEmpty());
  }

  @Test
  public void testDeleteKeepVersionZero() {
    byte[] qualifier = Bytes.toBytes("qualifier");

    long deleteTimestamp = 10;
    long valueTimestamp = 0;

    sdt.reset();
    KeyValue kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, deleteTimestamp,
        KeyValue.Type.Delete);
    sdt.add(kv);
    kv = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("f"), qualifier, valueTimestamp,
        KeyValue.Type.Delete);
    DeleteResult ret = sdt.isDeleted(kv);
    assertEquals(DeleteResult.NOT_DELETED, ret);
  }
}
