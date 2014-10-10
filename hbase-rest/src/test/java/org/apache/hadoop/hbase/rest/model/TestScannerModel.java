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

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestScannerModel extends TestModelBase<ScannerModel> {
  private static final String PRIVATE = "private";
  private static final String PUBLIC = "public";
  private static final byte[] START_ROW = Bytes.toBytes("abracadabra");
  private static final byte[] END_ROW = Bytes.toBytes("zzyzx");
  private static final byte[] COLUMN1 = Bytes.toBytes("column1");
  private static final byte[] COLUMN2 = Bytes.toBytes("column2:foo");
  private static final long START_TIME = 1245219839331L;
  private static final long END_TIME = 1245393318192L;
  private static final int CACHING = 1000;
  private static final int BATCH = 100;
  private static final boolean CACHE_BLOCKS = false;

  public TestScannerModel() throws Exception {
    super(ScannerModel.class);
    AS_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<Scanner batch=\"100\" cacheBlocks=\"false\" caching=\"1000\" endRow=\"enp5eng=\" "
        + "endTime=\"1245393318192\" maxVersions=\"2147483647\" startRow=\"YWJyYWNhZGFicmE=\" "
        + "startTime=\"1245219839331\">"
        + "<column>Y29sdW1uMQ==</column><column>Y29sdW1uMjpmb28=</column>"
        + "<labels>private</labels><labels>public</labels>"
        + "</Scanner>";

    AS_JSON = "{\"batch\":100,\"caching\":1000,\"cacheBlocks\":false,\"endRow\":\"enp5eng=\","
        + "\"endTime\":1245393318192,\"maxVersions\":2147483647,\"startRow\":\"YWJyYWNhZGFicmE=\","
        + "\"startTime\":1245219839331,\"column\":[\"Y29sdW1uMQ==\",\"Y29sdW1uMjpmb28=\"],"
        +"\"labels\":[\"private\",\"public\"]"
        +"}";

    AS_PB = "CgthYnJhY2FkYWJyYRIFenp5engaB2NvbHVtbjEaC2NvbHVtbjI6Zm9vIGQo47qL554kMLDi57mf"
        + "JDj/////B0joB1IHcHJpdmF0ZVIGcHVibGljWAA=";
  }

  protected ScannerModel buildTestModel() {
    ScannerModel model = new ScannerModel();
    model.setStartRow(START_ROW);
    model.setEndRow(END_ROW);
    model.addColumn(COLUMN1);
    model.addColumn(COLUMN2);
    model.setStartTime(START_TIME);
    model.setEndTime(END_TIME);
    model.setBatch(BATCH);
    model.setCaching(CACHING);
    model.addLabel(PRIVATE);
    model.addLabel(PUBLIC);
    model.setCacheBlocks(CACHE_BLOCKS);
    return model;
  }

  protected void checkModel(ScannerModel model) {
    assertTrue(Bytes.equals(model.getStartRow(), START_ROW));
    assertTrue(Bytes.equals(model.getEndRow(), END_ROW));
    boolean foundCol1 = false, foundCol2 = false;
    for (byte[] column : model.getColumns()) {
      if (Bytes.equals(column, COLUMN1)) {
        foundCol1 = true;
      } else if (Bytes.equals(column, COLUMN2)) {
        foundCol2 = true;
      }
    }
    assertTrue(foundCol1);
    assertTrue(foundCol2);
    assertEquals(model.getStartTime(), START_TIME);
    assertEquals(model.getEndTime(), END_TIME);
    assertEquals(model.getBatch(), BATCH);
    assertEquals(model.getCaching(), CACHING);
    assertEquals(model.getCacheBlocks(), CACHE_BLOCKS);
    boolean foundLabel1 = false;
    boolean foundLabel2 = false;
    if (model.getLabels() != null && model.getLabels().size() > 0) {
      for (String label : model.getLabels()) {
        if (label.equals(PRIVATE)) {
          foundLabel1 = true;
        } else if (label.equals(PUBLIC)) {
          foundLabel2 = true;
        }
      }
      assertTrue(foundLabel1);
      assertTrue(foundLabel2);
    }
  }

}
