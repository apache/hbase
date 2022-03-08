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
package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMobFileName {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobFileName.class);

  private String uuid;
  private Date date;
  private String dateStr;
  private byte[] startKey;

  @Before
  public void setUp() {
    uuid = HBaseTestingUtility.getRandomUUID().toString().replaceAll("-", "");
    date = new Date();
    dateStr = MobUtils.formatDate(date);
    startKey = Bytes.toBytes(ThreadLocalRandom.current().nextInt());
  }

  @Test
  public void testHashCode() {
    assertEquals(MobFileName.create(startKey, dateStr, uuid).hashCode(),
        MobFileName.create(startKey, dateStr, uuid).hashCode());
    assertNotSame(MobFileName.create(startKey, dateStr, uuid),
        MobFileName.create(startKey, dateStr, uuid));
  }

  @Test
  public void testCreate() {
    MobFileName mobFileName = MobFileName.create(startKey, dateStr, uuid);
    assertEquals(mobFileName, MobFileName.create(mobFileName.getFileName()));
  }

  @Test
  public void testGet() {
    MobFileName mobFileName = MobFileName.create(startKey, dateStr, uuid);
    assertEquals(MD5Hash.getMD5AsHex(startKey, 0, startKey.length), mobFileName.getStartKey());
    assertEquals(dateStr, mobFileName.getDate());
    assertEquals(mobFileName.getFileName(), MD5Hash.getMD5AsHex(startKey, 0, startKey.length)
        + dateStr + uuid);
  }

  @Test
  public void testEquals() {
    MobFileName mobFileName = MobFileName.create(startKey, dateStr, uuid);
    assertTrue(mobFileName.equals(mobFileName));
    assertFalse(mobFileName.equals(this));
    assertTrue(mobFileName.equals(MobFileName.create(startKey, dateStr, uuid)));
  }
}
