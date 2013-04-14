/**
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.ByteStreams;

// TODO: cover more test cases
@Category(SmallTests.class)
public class TestGet {

  private static final String WRITABLE_GET =
    "AgD//////////wAAAAEBD3Rlc3QuTW9ja0ZpbHRlcgEAAAAAAAAAAH//////////AQAAAAAAAAAA";

  private static final String MOCK_FILTER_JAR =
    "UEsDBBQACAgIACmBi0IAAAAAAAAAAAAAAAAJAAQATUVUQS1JTkYv/soAAAMAUEsHCAAAAAACAAAA" +
    "AAAAAFBLAwQUAAgICAApgYtCAAAAAAAAAAAAAAAAFAAAAE1FVEEtSU5GL01BTklGRVNULk1G803M" +
    "y0xLLS7RDUstKs7Mz7NSMNQz4OVyLkpNLElN0XWqBAmY6xnEG1gqaPgXJSbnpCo45xcV5BcllgCV" +
    "a/Jy8XIBAFBLBwgxyqRbQwAAAEQAAABQSwMECgAACAAAbICLQgAAAAAAAAAAAAAAAAUAAAB0ZXN0" +
    "L1BLAwQUAAgICAAcgItCAAAAAAAAAAAAAAAAFQAAAHRlc3QvTW9ja0ZpbHRlci5jbGFzc41Qy07C" +
    "QBS9A4VKBZGHoO7cgQvHmLjCuPBBQlJloWE/tCMdLZ1mOlV/y5WJCz/AjzLeDqCRYOIs7uuce87N" +
    "fHy+vQPAEezakCNQ1TzR9Ep6D30Raq5ssAh0pZpQFjMv4DRgvpQxDcYs4fTOcOiMeoYTAsUTEQl9" +
    "SiDf6Y4IWOfS5w7koVSGAhTRwBURv06nY65u2TjEjborPRaOmBJZPx9aOhAJgZq7dE+PgKM48/uC" +
    "hz4SWh33nj0yKiS9YJoNojjVvczYuXz2eKyFjBIb6gQaC9pg+I2gDVOTQwRXiBAoPCmh8Zb2b49h" +
    "qhcmzVUAet/IVHkcL8bt6s/xBxkb9gA/B7KXxwo/BaONHcVMMBf2X2HtBYscOBiLZliCdYzlGQFz" +
    "BTOBDagiaxNrC7uakTk2m4guS1SMRGsGziWyqgFN47xlsH+K1f4UaxuxbcPf+QJQSwcI8UIYqlEB" +
    "AABeAgAAUEsBAhQAFAAICAgAKYGLQgAAAAACAAAAAAAAAAkABAAAAAAAAAAAAAAAAAAAAE1FVEEt" +
    "SU5GL/7KAABQSwECFAAUAAgICAApgYtCMcqkW0MAAABEAAAAFAAAAAAAAAAAAAAAAAA9AAAATUVU" +
    "QS1JTkYvTUFOSUZFU1QuTUZQSwECCgAKAAAIAABsgItCAAAAAAAAAAAAAAAABQAAAAAAAAAAAAAA" +
    "AADCAAAAdGVzdC9QSwECFAAUAAgICAAcgItC8UIYqlEBAABeAgAAFQAAAAAAAAAAAAAAAADlAAAA" +
    "dGVzdC9Nb2NrRmlsdGVyLmNsYXNzUEsFBgAAAAAEAAQA8wAAAHkCAAAAAA==";

  @Test
  public void testAttributesSerialization() throws IOException {
    Get get = new Get();
    get.setAttribute("attribute1", Bytes.toBytes("value1"));
    get.setAttribute("attribute2", Bytes.toBytes("value2"));
    get.setAttribute("attribute3", Bytes.toBytes("value3"));

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteArrayOutputStream);
    get.write(out);

    Get get2 = new Get();
    Assert.assertTrue(get2.getAttributesMap().isEmpty());

    get2.readFields(new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));

    Assert.assertNull(get2.getAttribute("absent"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), get2.getAttribute("attribute1")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), get2.getAttribute("attribute2")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value3"), get2.getAttribute("attribute3")));
    Assert.assertEquals(3, get2.getAttributesMap().size());
  }

  @Test
  public void testGetAttributes() {
    Get get = new Get();
    Assert.assertTrue(get.getAttributesMap().isEmpty());
    Assert.assertNull(get.getAttribute("absent"));

    get.setAttribute("absent", null);
    Assert.assertTrue(get.getAttributesMap().isEmpty());
    Assert.assertNull(get.getAttribute("absent"));

    // adding attribute
    get.setAttribute("attribute1", Bytes.toBytes("value1"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), get.getAttribute("attribute1")));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), get.getAttributesMap().get("attribute1")));

    // overriding attribute value
    get.setAttribute("attribute1", Bytes.toBytes("value12"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), get.getAttribute("attribute1")));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), get.getAttributesMap().get("attribute1")));

    // adding another attribute
    get.setAttribute("attribute2", Bytes.toBytes("value2"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), get.getAttribute("attribute2")));
    Assert.assertEquals(2, get.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), get.getAttributesMap().get("attribute2")));

    // removing attribute
    get.setAttribute("attribute2", null);
    Assert.assertNull(get.getAttribute("attribute2"));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertNull(get.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    get.setAttribute("attribute2", null);
    Assert.assertNull(get.getAttribute("attribute2"));
    Assert.assertEquals(1, get.getAttributesMap().size());
    Assert.assertNull(get.getAttributesMap().get("attribute2"));

    // removing another attribute
    get.setAttribute("attribute1", null);
    Assert.assertNull(get.getAttribute("attribute1"));
    Assert.assertTrue(get.getAttributesMap().isEmpty());
    Assert.assertNull(get.getAttributesMap().get("attribute1"));
  }

  @Test
  public void testDynamicFilter() throws Exception {
    DataInput dis = ByteStreams.newDataInput(Base64.decode(WRITABLE_GET));
    Get get = new Get();
    try {
      get.readFields(dis);
      fail("Should not be able to load the filter class");
    } catch (RuntimeException re) {
      String msg = re.getMessage();
      Assert.assertTrue(msg != null
        && msg.contains("Can't find class test.MockFilter"));
    }

    Configuration conf = HBaseConfiguration.create();
    String localPath = conf.get("hbase.local.dir") + File.separator
      + "dynamic" + File.separator + "jars" + File.separator;
    File jarFile = new File(localPath, "MockFilter.jar");
    jarFile.deleteOnExit();

    FileOutputStream fos = new FileOutputStream(jarFile);
    fos.write(Base64.decode(MOCK_FILTER_JAR));
    fos.close();

    dis = ByteStreams.newDataInput(Base64.decode(WRITABLE_GET));
    get.readFields(dis);
    Assert.assertEquals("test.MockFilter",
      get.getFilter().getClass().getName());
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

