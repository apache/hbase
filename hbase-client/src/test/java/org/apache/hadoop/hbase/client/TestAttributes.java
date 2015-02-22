/**
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

package org.apache.hadoop.hbase.client;

import java.util.Arrays;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, SmallTests.class})
public class TestAttributes {
  private static final byte [] ROW = new byte [] {'r'};
  @Test
  public void testPutAttributes() {
    Put put = new Put(ROW);
    Assert.assertTrue(put.getAttributesMap().isEmpty());
    Assert.assertNull(put.getAttribute("absent"));

    put.setAttribute("absent", null);
    Assert.assertTrue(put.getAttributesMap().isEmpty());
    Assert.assertNull(put.getAttribute("absent"));

    // adding attribute
    put.setAttribute("attribute1", Bytes.toBytes("value1"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), put.getAttribute("attribute1")));
    Assert.assertEquals(1, put.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), put.getAttributesMap().get("attribute1")));

    // overriding attribute value
    put.setAttribute("attribute1", Bytes.toBytes("value12"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), put.getAttribute("attribute1")));
    Assert.assertEquals(1, put.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), put.getAttributesMap().get("attribute1")));

    // adding another attribute
    put.setAttribute("attribute2", Bytes.toBytes("value2"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), put.getAttribute("attribute2")));
    Assert.assertEquals(2, put.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), put.getAttributesMap().get("attribute2")));

    // removing attribute
    put.setAttribute("attribute2", null);
    Assert.assertNull(put.getAttribute("attribute2"));
    Assert.assertEquals(1, put.getAttributesMap().size());
    Assert.assertNull(put.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    put.setAttribute("attribute2", null);
    Assert.assertNull(put.getAttribute("attribute2"));
    Assert.assertEquals(1, put.getAttributesMap().size());
    Assert.assertNull(put.getAttributesMap().get("attribute2"));

    // removing another attribute
    put.setAttribute("attribute1", null);
    Assert.assertNull(put.getAttribute("attribute1"));
    Assert.assertTrue(put.getAttributesMap().isEmpty());
    Assert.assertNull(put.getAttributesMap().get("attribute1"));
  }


  @Test
  public void testDeleteAttributes() {
    Delete del = new Delete(new byte [] {'r'});
    Assert.assertTrue(del.getAttributesMap().isEmpty());
    Assert.assertNull(del.getAttribute("absent"));

    del.setAttribute("absent", null);
    Assert.assertTrue(del.getAttributesMap().isEmpty());
    Assert.assertNull(del.getAttribute("absent"));

    // adding attribute
    del.setAttribute("attribute1", Bytes.toBytes("value1"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), del.getAttribute("attribute1")));
    Assert.assertEquals(1, del.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), del.getAttributesMap().get("attribute1")));

    // overriding attribute value
    del.setAttribute("attribute1", Bytes.toBytes("value12"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), del.getAttribute("attribute1")));
    Assert.assertEquals(1, del.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), del.getAttributesMap().get("attribute1")));

    // adding another attribute
    del.setAttribute("attribute2", Bytes.toBytes("value2"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), del.getAttribute("attribute2")));
    Assert.assertEquals(2, del.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), del.getAttributesMap().get("attribute2")));

    // removing attribute
    del.setAttribute("attribute2", null);
    Assert.assertNull(del.getAttribute("attribute2"));
    Assert.assertEquals(1, del.getAttributesMap().size());
    Assert.assertNull(del.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    del.setAttribute("attribute2", null);
    Assert.assertNull(del.getAttribute("attribute2"));
    Assert.assertEquals(1, del.getAttributesMap().size());
    Assert.assertNull(del.getAttributesMap().get("attribute2"));

    // removing another attribute
    del.setAttribute("attribute1", null);
    Assert.assertNull(del.getAttribute("attribute1"));
    Assert.assertTrue(del.getAttributesMap().isEmpty());
    Assert.assertNull(del.getAttributesMap().get("attribute1"));
  }

  @Test
  public void testGetId() {
    Get get = new Get(ROW);
    Assert.assertNull("Make sure id is null if unset", get.toMap().get("id"));
    get.setId("myId");
    Assert.assertEquals("myId", get.toMap().get("id"));
  }

  @Test
  public void testAppendId() {
    Append append = new Append(ROW);
    Assert.assertNull("Make sure id is null if unset", append.toMap().get("id"));
    append.setId("myId");
    Assert.assertEquals("myId", append.toMap().get("id"));
  }

  @Test
  public void testDeleteId() {
    Delete delete = new Delete(ROW);
    Assert.assertNull("Make sure id is null if unset", delete.toMap().get("id"));
    delete.setId("myId");
    Assert.assertEquals("myId", delete.toMap().get("id"));
  }

  @Test
  public void testPutId() {
    Put put = new Put(ROW);
    Assert.assertNull("Make sure id is null if unset", put.toMap().get("id"));
    put.setId("myId");
    Assert.assertEquals("myId", put.toMap().get("id"));
  }

  @Test
  public void testScanId() {
    Scan scan = new Scan();
    Assert.assertNull("Make sure id is null if unset", scan.toMap().get("id"));
    scan.setId("myId");
    Assert.assertEquals("myId", scan.toMap().get("id"));
  }
}