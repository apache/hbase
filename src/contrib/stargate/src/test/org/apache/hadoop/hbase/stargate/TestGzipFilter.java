/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.stargate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.stargate.client.Client;
import org.apache.hadoop.hbase.stargate.client.Cluster;
import org.apache.hadoop.hbase.stargate.client.Response;
import org.apache.hadoop.hbase.util.Bytes;

public class TestGzipFilter extends MiniClusterTestCase {
  private static final String TABLE = "TestGzipFilter";
  private static final String CFA = "a";
  private static final String COLUMN_1 = CFA + ":1";
  private static final String COLUMN_2 = CFA + ":2";
  private static final String ROW_1 = "testrow1";
  private static final byte[] VALUE_1 = Bytes.toBytes("testvalue1");

  Client client;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(CFA));
    admin.createTable(htd);
    client = new Client(new Cluster().add("localhost", testServletPort));
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  public void testGzipFilter() throws Exception {
    String path = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1;

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GZIPOutputStream os = new GZIPOutputStream(bos);
    os.write(VALUE_1);
    os.close();
    byte[] value_1_gzip = bos.toByteArray();

    // input side filter

    Header[] headers = new Header[2];
    headers[0] = new Header("Content-Type", Constants.MIMETYPE_BINARY);
    headers[1] = new Header("Content-Encoding", "gzip");
    Response response = client.put(path, headers, value_1_gzip);
    assertEquals(response.getCode(), 200);

    HTable table = new HTable(conf, TABLE);
    Get get = new Get(Bytes.toBytes(ROW_1));
    get.addColumn(Bytes.toBytes(CFA), Bytes.toBytes("1"));
    Result result = table.get(get);
    byte[] value = result.getValue(Bytes.toBytes(CFA), Bytes.toBytes("1"));
    assertNotNull(value);
    assertTrue(Bytes.equals(value, VALUE_1));

    // output side filter

    headers[0] = new Header("Accept", Constants.MIMETYPE_BINARY);
    headers[1] = new Header("Accept-Encoding", "gzip");
    response = client.get(path, headers);
    assertEquals(response.getCode(), 200);
    ByteArrayInputStream bis = new ByteArrayInputStream(response.getBody());
    GZIPInputStream is = new GZIPInputStream(bis);
    value = new byte[VALUE_1.length];
    is.read(value, 0, VALUE_1.length);
    assertTrue(Bytes.equals(value, VALUE_1));
    is.close();

    // errors should not be compressed

    String badPath = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_2;
    headers[0] = new Header("Accept", Constants.MIMETYPE_BINARY);
    headers[1] = new Header("Accept-Encoding", "gzip");
    response = client.get(badPath, headers);
    assertEquals(response.getCode(), 404);
    String contentEncoding = response.getHeader("Content-Encoding");
    assertTrue(contentEncoding == null || !contentEncoding.contains("gzip"));
  }
}