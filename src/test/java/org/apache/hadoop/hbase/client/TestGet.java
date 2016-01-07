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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
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

  private static final String WRITABLE_GET_WITH_FILTER_LIST =
    "AgD//////////wAAAAEBKW9yZy5hcGFjaGUuaGFkb29wLmhiYXNlLmZpbHRlci5GaWx0ZXJMaXN0" +
    "AAAAAAMOAA90ZXN0Lk1vY2tGaWx0ZXIOAA1teS5Nb2NrRmlsdGVyDkYAAQAAAAAAAAAAf///////" +
    "//8BAAAAAAAAAAA=";

  private static final String MOCK_FILTER_JAR =
    "UEsDBBQACAgIADRQI0QAAAAAAAAAAAAAAAAJAAQATUVUQS1JTkYv/soAAAMAUEsHCAAAAAACAAAA" +
    "AAAAAFBLAwQUAAgICAA0UCNEAAAAAAAAAAAAAAAAFAAAAE1FVEEtSU5GL01BTklGRVNULk1G803M" +
    "y0xLLS7RDUstKs7Mz7NSMNQz4OVyLkpNLElN0XWqBAmY6xnEG1gqaPgXJSbnpCo45xcV5BcllgCV" +
    "a/Jy8XIBAFBLBwgxyqRbQwAAAEQAAABQSwMEFAAICAgAcIsiRAAAAAAAAAAAAAAAABMAAABteS9N" +
    "b2NrRmlsdGVyLmNsYXNzjVDLTsJAFL1DC5UKIg9B3bkDF4wxcYVx4YOEBGWhYT+0Ix0tnWaY+vgs" +
    "VyYu/AA/yng7gAaDibO4r3PuOTfz8fn2DgCHsOtAhkBx8kwvpXffFaHmygGbQEuqMWUx8wJOA+ZL" +
    "GdNgxKac3hoOnVFPcUIgdywioU8IWM3WkIB9Jn3uggX5AmQhR6DUFxG/SiYjrm7YKMSNSl96LBwy" +
    "JdJ+PrR1IKYpe+maDgFXceZ3BQ99hOvN/h17YFRIes4060VxojuprXvx5PFYCxlNHagQqC5ovcE3" +
    "giZMjQ8QXCFCIPuohMZLGsseg0QvTGqrAPS+lonyOF6M26Wf49spG/YAvwbSZ2GFX4LRwY5iJpiz" +
    "+6+w9oJFBlyMOTPMwzrGwoyAuYiZwAaUkLWJtY1d2cgcmU1Ef0sUjUR9Bs4l0qoKNeO8ZbB/ipX/" +
    "FGsYsW3D3/kCUEsHCEYmW6RQAQAAWgIAAFBLAwQUAAgICABuiyJEAAAAAAAAAAAAAAAAFQAAAHRl" +
    "c3QvTW9ja0ZpbHRlci5jbGFzc41Qy07CQBS9A4VKBZGHoO7cgQvHmLjCuPBBQlJloWE/tCMdLZ1m" +
    "OlV/y5WJCz/AjzLeDqCRYOIs7uuce87NfHy+vQPAEezakCNQ1TzR9Ep6D30Raq5ssAh0pZpQFjMv" +
    "4DRgvpQxDcYs4fTOcOiMeoYTAsUTEQl9SiDf6Y4IWOfS5w7koVSGAhTRwBURv06nY65u2TjEjbor" +
    "PRaOmBJZPx9aOhAJgZq7dE+PgKM48/uChz4SWh33nj0yKiS9YJoNojjVvczYuXz2eKyFjBIb6gQa" +
    "C9pg+I2gDVOTQwRXiBAoPCmh8Zb2b49hqhcmzVUAet/IVHkcL8bt6s/xBxkb9gA/B7KXxwo/BaON" +
    "HcVMMBf2X2HtBYscOBiLZliCdYzlGQFzBTOBDagiaxNrC7uakTk2m4guS1SMRGsGziWyqgFN47xl" +
    "sH+K1f4UaxuxbcPf+QJQSwcI8UIYqlEBAABeAgAAUEsBAhQAFAAICAgANFAjRAAAAAACAAAAAAAA" +
    "AAkABAAAAAAAAAAAAAAAAAAAAE1FVEEtSU5GL/7KAABQSwECFAAUAAgICAA0UCNEMcqkW0MAAABE" +
    "AAAAFAAAAAAAAAAAAAAAAAA9AAAATUVUQS1JTkYvTUFOSUZFU1QuTUZQSwECFAAUAAgICABwiyJE" +
    "RiZbpFABAABaAgAAEwAAAAAAAAAAAAAAAADCAAAAbXkvTW9ja0ZpbHRlci5jbGFzc1BLAQIUABQA" +
    "CAgIAG6LIkTxQhiqUQEAAF4CAAAVAAAAAAAAAAAAAAAAAFMCAAB0ZXN0L01vY2tGaWx0ZXIuY2xh" +
    "c3NQSwUGAAAAAAQABAABAQAA5wMAAAAA";

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
    Configuration conf = HBaseConfiguration.create();
    String localPath = conf.get("hbase.local.dir")
      + File.separator + "jars" + File.separator;
    File jarFile = new File(localPath, "MockFilter.jar");
    jarFile.delete();
    assertFalse("Should be deleted: " + jarFile.getPath(), jarFile.exists());

    DataInput dis = ByteStreams.newDataInput(Base64.decode(WRITABLE_GET));
    Get get = new Get();
    try {
      get.readFields(dis);
      fail("Should not be able to load the filter class");
    } catch (RuntimeException re) {
      String msg = re.getMessage();
      assertTrue(msg != null && msg.contains("Can't find class test.MockFilter"));
    }

    dis = ByteStreams.newDataInput(Base64.decode(WRITABLE_GET_WITH_FILTER_LIST));
    try {
      get.readFields(dis);
      fail("Should not be able to load the filter class");
    } catch (IOException ioe) {
      assertTrue(ioe.getCause() instanceof ClassNotFoundException);
    }

    FileOutputStream fos = new FileOutputStream(jarFile);
    fos.write(Base64.decode(MOCK_FILTER_JAR));
    fos.close();

    dis = ByteStreams.newDataInput(Base64.decode(WRITABLE_GET));
    get.readFields(dis);
    assertEquals("test.MockFilter", get.getFilter().getClass().getName());

    get = new Get();
    dis = ByteStreams.newDataInput(Base64.decode(WRITABLE_GET_WITH_FILTER_LIST));
    get.readFields(dis);
    assertTrue(get.getFilter() instanceof FilterList);
    List<Filter> filters = ((FilterList)get.getFilter()).getFilters();
    assertEquals(3, filters.size());
    assertEquals("test.MockFilter", filters.get(0).getClass().getName());
    assertEquals("my.MockFilter", filters.get(1).getClass().getName());
    assertTrue(filters.get(2) instanceof KeyOnlyFilter);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

