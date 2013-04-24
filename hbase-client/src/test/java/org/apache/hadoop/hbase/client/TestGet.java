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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// TODO: cover more test cases
@Category(SmallTests.class)
public class TestGet {
  private static final byte [] ROW = new byte [] {'r'};

  private static final String PB_GET = "CgNyb3ciEwoPdGVzdC5Nb2NrRmlsdGVyEgAwATgB";

  private static final String MOCK_FILTER_JAR =
    "UEsDBBQACAgIACqBiEIAAAAAAAAAAAAAAAAJAAQATUVUQS1JTkYv/soAAAMAUEsHCAAAAAACAAAA" +
    "AAAAAFBLAwQUAAgICAAqgYhCAAAAAAAAAAAAAAAAFAAAAE1FVEEtSU5GL01BTklGRVNULk1G803M" +
    "y0xLLS7RDUstKs7Mz7NSMNQz4OVyLkpNLElN0XWqBAmY6xnEG1gqaPgXJSbnpCo45xcV5BcllgCV" +
    "a/Jy8XIBAFBLBwgxyqRbQwAAAEQAAABQSwMECgAACAAAz4CIQgAAAAAAAAAAAAAAAAUAAAB0ZXN0" +
    "L1BLAwQUAAgICACPgIhCAAAAAAAAAAAAAAAAFQAAAHRlc3QvTW9ja0ZpbHRlci5jbGFzc41Qy0rD" +
    "QBQ9k6RNG6N9aH2uXAhWwUC3FRdRC0J1oxSkq0k6mmjaCUkq6lfpqqLgB/hR4k1aqlQEs7j3zLnn" +
    "3Ec+Pl/fATSwoUNhKCUiTqxT6d62/CARkQ6NoS6ja4uH3PWE5fGelKHlOTwW1lWmscZSmxiG/L4/" +
    "8JMDBnW73mHQDmVPGFBRNJFDnga0/YE4G/YdEV1wJyBHtS1dHnR45KfvCaklnh8zVNoz+zQZiiGP" +
    "YtGKZJ+htt216780BkjFoIeO/UA1BqVrM+xm2n+dQlOM43tXhIkvB7GOZYbmX0Yx1VlHIhZ0ReA/" +
    "8pSYdkj3WTWxgBL1PZfDyBU0h64sfS+9d8PvODZJqSL9VEL0wyjq9LIoM8q5nREKzwQUGBTzYxJz" +
    "FM0JNjFPuZhOm5gbpE5rhTewyxHKTzN+/Ye/gAqqQPmE/IukWiJOo0ot67Q1XeMFK7NtWNZGydBa" +
    "hta/AFBLBwjdsJqTXwEAAF0CAABQSwECFAAUAAgICAAqgYhCAAAAAAIAAAAAAAAACQAEAAAAAAAA" +
    "AAAAAAAAAAAATUVUQS1JTkYv/soAAFBLAQIUABQACAgIACqBiEIxyqRbQwAAAEQAAAAUAAAAAAAA" +
    "AAAAAAAAAD0AAABNRVRBLUlORi9NQU5JRkVTVC5NRlBLAQIKAAoAAAgAAM+AiEIAAAAAAAAAAAAA" +
    "AAAFAAAAAAAAAAAAAAAAAMIAAAB0ZXN0L1BLAQIUABQACAgIAI+AiELdsJqTXwEAAF0CAAAVAAAA" +
    "AAAAAAAAAAAAAOUAAAB0ZXN0L01vY2tGaWx0ZXIuY2xhc3NQSwUGAAAAAAQABADzAAAAhwIAAAAA";

  @Test
  public void testAttributesSerialization() throws IOException {
    Get get = new Get(Bytes.toBytes("row"));
    get.setAttribute("attribute1", Bytes.toBytes("value1"));
    get.setAttribute("attribute2", Bytes.toBytes("value2"));
    get.setAttribute("attribute3", Bytes.toBytes("value3"));

    ClientProtos.Get getProto = ProtobufUtil.toGet(get);

    Get get2 = ProtobufUtil.toGet(getProto);
    Assert.assertNull(get2.getAttribute("absent"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), get2.getAttribute("attribute1")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), get2.getAttribute("attribute2")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value3"), get2.getAttribute("attribute3")));
    Assert.assertEquals(3, get2.getAttributesMap().size());
  }

  @Test
  public void testGetAttributes() {
    Get get = new Get(ROW);
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
  public void testNullQualifier() {
    Get get = new Get(ROW);
    byte[] family = Bytes.toBytes("family");
    get.addColumn(family, null);
    Set<byte[]> qualifiers = get.getFamilyMap().get(family);
    Assert.assertEquals(1, qualifiers.size());
  }

  @Test
  public void testDynamicFilter() throws Exception {
    ClientProtos.Get getProto = ClientProtos.Get.parseFrom(Base64.decode(PB_GET));
    try {
      ProtobufUtil.toGet(getProto);
      fail("Should not be able to load the filter class");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getCause() instanceof ClassNotFoundException);
    }

    Configuration conf = HBaseConfiguration.create();
    String localPath = conf.get("hbase.local.dir")
      + File.separator + "jars" + File.separator;
    File jarFile = new File(localPath, "MockFilter.jar");
    jarFile.deleteOnExit();

    FileOutputStream fos = new FileOutputStream(jarFile);
    fos.write(Base64.decode(MOCK_FILTER_JAR));
    fos.close();

    Get get = ProtobufUtil.toGet(getProto);
    Assert.assertEquals("test.MockFilter",
      get.getFilter().getClass().getName());
  }
}
