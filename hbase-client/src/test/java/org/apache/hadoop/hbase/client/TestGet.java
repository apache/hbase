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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// TODO: cover more test cases
@Category({ClientTests.class, SmallTests.class})
public class TestGet {
  private static final byte [] ROW = new byte [] {'r'};

  private static final String PB_GET = "CgNyb3ciEwoPdGVzdC5Nb2NrRmlsdGVyEgAwATgB";
  private static final String PB_GET_WITH_FILTER_LIST =
    "CgFyIosBCilvcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuRmlsdGVyTGlzdBJeCAESEwoP" +
    "dGVzdC5Nb2NrRmlsdGVyEgASEQoNbXkuTW9ja0ZpbHRlchIAEjIKLG9yZy5hcGFjaGUuaGFkb29w" +
    "LmhiYXNlLmZpbHRlci5LZXlPbmx5RmlsdGVyEgIIADABOAE=";

  private static final String MOCK_FILTER_JAR =
    "UEsDBBQACAgIANWDlEMAAAAAAAAAAAAAAAAJAAQATUVUQS1JTkYv/soAAAMAUEsHCAAAAAACAAAA" +
    "AAAAAFBLAwQUAAgICADVg5RDAAAAAAAAAAAAAAAAFAAAAE1FVEEtSU5GL01BTklGRVNULk1G803M" +
    "y0xLLS7RDUstKs7Mz7NSMNQz4OVyLkpNLElN0XWqBAmY6xnEG1gqaPgXJSbnpCo45xcV5BcllgCV" +
    "a/Jy8XIBAFBLBwgxyqRbQwAAAEQAAABQSwMEFAAICAgAUoOUQwAAAAAAAAAAAAAAABMAAABteS9N" +
    "b2NrRmlsdGVyLmNsYXNzdZHPTsJAEMa/LYVCRVFQMd68gQc38YrxUJUTetGQGE7bstrVwjbbYsSn" +
    "0hOJJj6AD2WcFoP/4iYzX+bb32xmd9/en18B7GPLhY11BxsurEw3GUoHaqzSQ4ZCq91nsI/0UDLU" +
    "emoszyYjX5oL4Ufk1Hs6EFFfGJXVn6adhirJ6NGUn+rgtquiVJoOQyUWJpFdo0cMjdbAa/8hnNj3" +
    "pqmkbmvgMbgn94GMU6XHiYMm1ed6YgJJeDbNV+fejbgTVRRRYlj+cSZDW5trLmIRhJKHYqh1zENf" +
    "JJJf5QCfcx45DJ3/WLmYgx/LRNJ1I/UgMmMxIXbo9WxkywLLZqHsUMVJGWlxdwb2lG+XKZdys4kK" +
    "5eocgIsl0grVy0Q5+e9Y+V75BdblDIXHX/3b3/rLWEGNdJXCJmeNop7zjQ9QSwcI1kzyMToBAADs" +
    "AQAAUEsDBBQACAgIAFKDlEMAAAAAAAAAAAAAAAAVAAAAdGVzdC9Nb2NrRmlsdGVyLmNsYXNzdVHB" +
    "TsJAFJwthUJFERQx3ryBBzfxivFQlRN60ZAYTtuy2tXCNtti1K/SE4kmfoAfZXwtBg3RTd6bzOy8" +
    "zezux+frO4ADbLuwsemg6cLKcIuhdKgmKj1iKLQ7Awb7WI8kQ62vJvJ8OvaluRR+REqjrwMRDYRR" +
    "Gf8W7TRUCUO9n8ok5Wc6uOupKJWmy1CJhUlkz+gxQ7M99Dp/eJzY9x5JZrCGHoN7+hDIOFV6kjho" +
    "Eb/QUxNIsmeJfib3b8W9qKKIEslLpzJ0tLnhIhZBKHkoRlrHPPRFIvl1buBzn0cKQ/c/r1wk4Scy" +
    "kXTpSD2JTFhkxC69oY1sWWBZGuoOMU7ICIt7M7CXfLtMvZSLLVSoV+cGuFghrBBfJZeT/5GV75Xf" +
    "YF3NUHhemt/5NV/GGmqE61Q2KXWqRu7f+AJQSwcIrS5nKDoBAADyAQAAUEsBAhQAFAAICAgA1YOU" +
    "QwAAAAACAAAAAAAAAAkABAAAAAAAAAAAAAAAAAAAAE1FVEEtSU5GL/7KAABQSwECFAAUAAgICADV" +
    "g5RDMcqkW0MAAABEAAAAFAAAAAAAAAAAAAAAAAA9AAAATUVUQS1JTkYvTUFOSUZFU1QuTUZQSwEC" +
    "FAAUAAgICABSg5RD1kzyMToBAADsAQAAEwAAAAAAAAAAAAAAAADCAAAAbXkvTW9ja0ZpbHRlci5j" +
    "bGFzc1BLAQIUABQACAgIAFKDlEOtLmcoOgEAAPIBAAAVAAAAAAAAAAAAAAAAAD0CAAB0ZXN0L01v" +
    "Y2tGaWx0ZXIuY2xhc3NQSwUGAAAAAAQABAABAQAAugMAAAAA";

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
  public void TestGetRowFromGetCopyConstructor() throws Exception {
    Get get = new Get(ROW);
    get.setFilter(null);
    get.setAuthorizations(new Authorizations("foo"));
    get.setACL("u", new Permission(Permission.Action.READ));
    get.setConsistency(Consistency.TIMELINE);
    get.setReplicaId(2);
    get.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    get.setCheckExistenceOnly(true);
    get.setTimeRange(3, 4);
    get.setMaxVersions(11);
    get.setMaxResultsPerColumnFamily(10);
    get.setRowOffsetPerColumnFamily(11);
    get.setCacheBlocks(true);

    Get copyGet = new Get(get);
    assertEquals(0, Bytes.compareTo(get.getRow(), copyGet.getRow()));

    // from OperationWithAttributes
    assertEquals(get.getId(), copyGet.getId());

    // from Query class
    assertEquals(get.getFilter(), copyGet.getFilter());
    assertTrue(get.getAuthorizations().toString().equals(copyGet.getAuthorizations().toString()));
    assertTrue(Bytes.equals(get.getACL(), copyGet.getACL()));
    assertEquals(get.getConsistency(), copyGet.getConsistency());
    assertEquals(get.getReplicaId(), copyGet.getReplicaId());
    assertEquals(get.getIsolationLevel(), copyGet.getIsolationLevel());

    // from Get class
    assertEquals(get.isCheckExistenceOnly(), copyGet.isCheckExistenceOnly());
    assertTrue(get.getTimeRange().equals(copyGet.getTimeRange()));
    assertEquals(get.getMaxVersions(), copyGet.getMaxVersions());
    assertEquals(get.getMaxResultsPerColumnFamily(), copyGet.getMaxResultsPerColumnFamily());
    assertEquals(get.getRowOffsetPerColumnFamily(), copyGet.getRowOffsetPerColumnFamily());
    assertEquals(get.getCacheBlocks(), copyGet.getCacheBlocks());
    assertEquals(get.getId(), copyGet.getId());
  }

  @Test
  public void testDynamicFilter() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String localPath = conf.get("hbase.local.dir")
      + File.separator + "jars" + File.separator;
    File jarFile = new File(localPath, "MockFilter.jar");
    jarFile.delete();
    assertFalse("Should be deleted: " + jarFile.getPath(), jarFile.exists());

    ClientProtos.Get getProto1 =
      ClientProtos.Get.parseFrom(Base64.decode(PB_GET));
    ClientProtos.Get getProto2 =
      ClientProtos.Get.parseFrom(Base64.decode(PB_GET_WITH_FILTER_LIST));
    try {
      ProtobufUtil.toGet(getProto1);
      fail("Should not be able to load the filter class");
    } catch (IOException ioe) {
      assertTrue(ioe.getCause() instanceof ClassNotFoundException);
    }
    try {
      ProtobufUtil.toGet(getProto2);
      fail("Should not be able to load the filter class");
    } catch (IOException ioe) {
      assertTrue(ioe.getCause() instanceof InvocationTargetException);
      InvocationTargetException ite = (InvocationTargetException)ioe.getCause();
      assertTrue(ite.getTargetException()
        instanceof DeserializationException);
    }
    FileOutputStream fos = new FileOutputStream(jarFile);
    fos.write(Base64.decode(MOCK_FILTER_JAR));
    fos.close();

    Get get1 = ProtobufUtil.toGet(getProto1);
    assertEquals("test.MockFilter", get1.getFilter().getClass().getName());

    Get get2 = ProtobufUtil.toGet(getProto2);
    assertTrue(get2.getFilter() instanceof FilterList);
    List<Filter> filters = ((FilterList)get2.getFilter()).getFilters();
    assertEquals(3, filters.size());
    assertEquals("test.MockFilter", filters.get(0).getClass().getName());
    assertEquals("my.MockFilter", filters.get(1).getClass().getName());
    assertTrue(filters.get(2) instanceof KeyOnlyFilter);
  }
}
