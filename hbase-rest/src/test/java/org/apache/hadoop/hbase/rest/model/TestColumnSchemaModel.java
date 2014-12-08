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

package org.apache.hadoop.hbase.rest.model;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestColumnSchemaModel extends TestModelBase<ColumnSchemaModel> {

  protected static final String COLUMN_NAME = "testcolumn";
  protected static final boolean BLOCKCACHE = true;
  protected static final int BLOCKSIZE = 16384;
  protected static final String BLOOMFILTER = "NONE";
  protected static final String COMPRESSION = "GZ";
  protected static final boolean IN_MEMORY = false;
  protected static final int TTL = 86400;
  protected static final int VERSIONS = 1;

  public TestColumnSchemaModel() throws Exception {
    super(ColumnSchemaModel.class);
    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ColumnSchema " +
          "name=\"testcolumn\" BLOCKSIZE=\"16384\" BLOOMFILTER=\"NONE\" BLOCKCACHE=\"true\" " +
          "COMPRESSION=\"GZ\" VERSIONS=\"1\" TTL=\"86400\" IN_MEMORY=\"false\"/>";

    AS_JSON =
      "{\"name\":\"testcolumn\",\"BLOCKSIZE\":\"16384\",\"BLOOMFILTER\":\"NONE\"," +
          "\"BLOCKCACHE\":\"true\",\"COMPRESSION\":\"GZ\",\"VERSIONS\":\"1\"," +
          "\"TTL\":\"86400\",\"IN_MEMORY\":\"false\"}";
  }

  protected ColumnSchemaModel buildTestModel() {
    ColumnSchemaModel model = new ColumnSchemaModel();
    model.setName(COLUMN_NAME);
    model.__setBlocksize(BLOCKSIZE);
    model.__setBloomfilter(BLOOMFILTER);
    model.__setBlockcache(BLOCKCACHE);
    model.__setCompression(COMPRESSION);
    model.__setVersions(VERSIONS);
    model.__setTTL(TTL);
    model.__setInMemory(IN_MEMORY);
    return model;
  }

  protected void checkModel(ColumnSchemaModel model) {
    assertEquals(model.getName(), COLUMN_NAME);
    assertEquals(model.__getBlockcache(), BLOCKCACHE);
    assertEquals(model.__getBlocksize(), BLOCKSIZE);
    assertEquals(model.__getBloomfilter(), BLOOMFILTER);
    assertTrue(model.__getCompression().equalsIgnoreCase(COMPRESSION));
    assertEquals(model.__getInMemory(), IN_MEMORY);
    assertEquals(model.__getTTL(), TTL);
    assertEquals(model.__getVersions(), VERSIONS);
  }

  public void testFromPB() throws Exception {
  }
}

