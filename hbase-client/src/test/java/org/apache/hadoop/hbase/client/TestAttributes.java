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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestAttributes {

  private static final byte[] ROW = new byte[] { 'r' };

  @Test
  public void testPutAttributes() {
    Put put = new Put(ROW);
    assertTrue(put.getAttributesMap().isEmpty());
    assertNull(put.getAttribute("absent"));

    put.setAttribute("absent", null);
    assertTrue(put.getAttributesMap().isEmpty());
    assertNull(put.getAttribute("absent"));

    // adding attribute
    put.setAttribute("attribute1", Bytes.toBytes("value1"));
    assertTrue(Arrays.equals(Bytes.toBytes("value1"), put.getAttribute("attribute1")));
    assertEquals(1, put.getAttributesMap().size());
    assertTrue(Arrays.equals(Bytes.toBytes("value1"), put.getAttributesMap().get("attribute1")));

    // overriding attribute value
    put.setAttribute("attribute1", Bytes.toBytes("value12"));
    assertTrue(Arrays.equals(Bytes.toBytes("value12"), put.getAttribute("attribute1")));
    assertEquals(1, put.getAttributesMap().size());
    assertTrue(Arrays.equals(Bytes.toBytes("value12"), put.getAttributesMap().get("attribute1")));

    // adding another attribute
    put.setAttribute("attribute2", Bytes.toBytes("value2"));
    assertTrue(Arrays.equals(Bytes.toBytes("value2"), put.getAttribute("attribute2")));
    assertEquals(2, put.getAttributesMap().size());
    assertTrue(Arrays.equals(Bytes.toBytes("value2"), put.getAttributesMap().get("attribute2")));

    // removing attribute
    put.setAttribute("attribute2", null);
    assertNull(put.getAttribute("attribute2"));
    assertEquals(1, put.getAttributesMap().size());
    assertNull(put.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    put.setAttribute("attribute2", null);
    assertNull(put.getAttribute("attribute2"));
    assertEquals(1, put.getAttributesMap().size());
    assertNull(put.getAttributesMap().get("attribute2"));

    // removing another attribute
    put.setAttribute("attribute1", null);
    assertNull(put.getAttribute("attribute1"));
    assertTrue(put.getAttributesMap().isEmpty());
    assertNull(put.getAttributesMap().get("attribute1"));
  }

  @Test
  public void testDeleteAttributes() {
    Delete del = new Delete(new byte[] { 'r' });
    assertTrue(del.getAttributesMap().isEmpty());
    assertNull(del.getAttribute("absent"));

    del.setAttribute("absent", null);
    assertTrue(del.getAttributesMap().isEmpty());
    assertNull(del.getAttribute("absent"));

    // adding attribute
    del.setAttribute("attribute1", Bytes.toBytes("value1"));
    assertTrue(Arrays.equals(Bytes.toBytes("value1"), del.getAttribute("attribute1")));
    assertEquals(1, del.getAttributesMap().size());
    assertTrue(Arrays.equals(Bytes.toBytes("value1"), del.getAttributesMap().get("attribute1")));

    // overriding attribute value
    del.setAttribute("attribute1", Bytes.toBytes("value12"));
    assertTrue(Arrays.equals(Bytes.toBytes("value12"), del.getAttribute("attribute1")));
    assertEquals(1, del.getAttributesMap().size());
    assertTrue(Arrays.equals(Bytes.toBytes("value12"), del.getAttributesMap().get("attribute1")));

    // adding another attribute
    del.setAttribute("attribute2", Bytes.toBytes("value2"));
    assertTrue(Arrays.equals(Bytes.toBytes("value2"), del.getAttribute("attribute2")));
    assertEquals(2, del.getAttributesMap().size());
    assertTrue(Arrays.equals(Bytes.toBytes("value2"), del.getAttributesMap().get("attribute2")));

    // removing attribute
    del.setAttribute("attribute2", null);
    assertNull(del.getAttribute("attribute2"));
    assertEquals(1, del.getAttributesMap().size());
    assertNull(del.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    del.setAttribute("attribute2", null);
    assertNull(del.getAttribute("attribute2"));
    assertEquals(1, del.getAttributesMap().size());
    assertNull(del.getAttributesMap().get("attribute2"));

    // removing another attribute
    del.setAttribute("attribute1", null);
    assertNull(del.getAttribute("attribute1"));
    assertTrue(del.getAttributesMap().isEmpty());
    assertNull(del.getAttributesMap().get("attribute1"));
  }

  @Test
  public void testGetId() {
    Get get = new Get(ROW);
    assertNull(get.toMap().get("id"), "Make sure id is null if unset");
    get.setId("myId");
    assertEquals("myId", get.toMap().get("id"));
  }

  @Test
  public void testAppendId() {
    Append append = new Append(ROW);
    assertNull(append.toMap().get("id"), "Make sure id is null if unset");
    append.setId("myId");
    assertEquals("myId", append.toMap().get("id"));
  }

  @Test
  public void testDeleteId() {
    Delete delete = new Delete(ROW);
    assertNull(delete.toMap().get("id"), "Make sure id is null if unset");
    delete.setId("myId");
    assertEquals("myId", delete.toMap().get("id"));
  }

  @Test
  public void testPutId() {
    Put put = new Put(ROW);
    assertNull(put.toMap().get("id"), "Make sure id is null if unset");
    put.setId("myId");
    assertEquals("myId", put.toMap().get("id"));
  }

  @Test
  public void testScanId() {
    Scan scan = new Scan();
    assertNull(scan.toMap().get("id"), "Make sure id is null if unset");
    scan.setId("myId");
    assertEquals("myId", scan.toMap().get("id"));
  }
}
