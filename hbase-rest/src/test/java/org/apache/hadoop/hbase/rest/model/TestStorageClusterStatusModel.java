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
package org.apache.hadoop.hbase.rest.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestStorageClusterStatusModel extends TestModelBase<StorageClusterStatusModel> {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStorageClusterStatusModel.class);

  public TestStorageClusterStatusModel() throws Exception {
    super(StorageClusterStatusModel.class);

    AS_XML =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<ClusterStatus averageLoad=\"1.0\" regions=\"2\" requests=\"0\">" +
            "<DeadNodes/><LiveNodes>" +
            "<Node heapSizeMB=\"128\" maxHeapSizeMB=\"1024\" name=\"test1\" requests=\"0\" " +
            "startCode=\"1245219839331\"><Region currentCompactedKVs=\"1\" memstoreSizeMB=\"0\" " +
            "name=\"aGJhc2U6cm9vdCwsMA==\" readRequestsCount=\"1\" rootIndexSizeKB=\"1\" " +
            "storefileIndexSizeKB=\"0\" storefileSizeMB=\"0\" storefiles=\"1\" stores=\"1\" " +
            "totalCompactingKVs=\"1\" totalStaticBloomSizeKB=\"1\" totalStaticIndexSizeKB=\"1\" " +
            "writeRequestsCount=\"2\"/></Node>" +
            "<Node heapSizeMB=\"512\" maxHeapSizeMB=\"1024\" name=\"test2\" requests=\"0\" " +
            "startCode=\"1245239331198\">" +
            "<Region currentCompactedKVs=\"1\" memstoreSizeMB=\"0\" " +
            "name=\"aGJhc2U6bWV0YSwsMTI0NjAwMDA0MzcyNA==\" readRequestsCount=\"1\" " +
            "rootIndexSizeKB=\"1\" storefileIndexSizeKB=\"0\" storefileSizeMB=\"0\" " +
            "storefiles=\"1\" stores=\"1\" totalCompactingKVs=\"1\" totalStaticBloomSizeKB=\"1\" " +
            "totalStaticIndexSizeKB=\"1\" writeRequestsCount=\"2\"/></Node></LiveNodes>" +
            "</ClusterStatus>";

    AS_PB =
      "Cj8KBXRlc3QxEOO6i+eeJBgAIIABKIAIMicKDWhiYXNlOnJvb3QsLDAQARgBIAAoADAAOAFAAkgB" +
      "UAFYAWABaAEKSwoFdGVzdDIQ/pKx8J4kGAAggAQogAgyMwoZaGJhc2U6bWV0YSwsMTI0NjAwMDA0" +
      "MzcyNBABGAEgACgAMAA4AUACSAFQAVgBYAFoARgCIAApAAAAAAAA8D8=";


    //Using jackson will break json backward compatibilty for this representation
    //but the original one was broken as it would only print one Node element
    //so the format itself was broken
    AS_JSON =
      "{\"regions\":2,\"requests\":0,\"averageLoad\":1.0,\"LiveNodes\":[{\"name\":\"test1\"," +
          "\"Region\":[{\"name\":\"aGJhc2U6cm9vdCwsMA==\",\"stores\":1,\"storefiles\":1," +
          "\"storefileSizeMB\":0,\"memStoreSizeMB\":0,\"storefileIndexSizeKB\":0," +
          "\"readRequestsCount\":1,\"writeRequestsCount\":2,\"rootIndexSizeKB\":1," +
          "\"totalStaticIndexSizeKB\":1,\"totalStaticBloomSizeKB\":1,\"totalCompactingKVs\":1," +
          "\"currentCompactedKVs\":1}],\"requests\":0,\"startCode\":1245219839331," +
          "\"heapSizeMB\":128,\"maxHeapSizeMB\":1024},{\"name\":\"test2\"," +
          "\"Region\":[{\"name\":\"aGJhc2U6bWV0YSwsMTI0NjAwMDA0MzcyNA==\",\"stores\":1," +
          "\"storefiles\":1,\"storefileSizeMB\":0,\"memStoreSizeMB\":0,\"storefileIndexSizeKB\":0," +
          "\"readRequestsCount\":1,\"writeRequestsCount\":2,\"rootIndexSizeKB\":1," +
          "\"totalStaticIndexSizeKB\":1,\"totalStaticBloomSizeKB\":1,\"totalCompactingKVs\":1," +
          "\"currentCompactedKVs\":1}],\"requests\":0,\"startCode\":1245239331198," +
          "\"heapSizeMB\":512,\"maxHeapSizeMB\":1024}],\"DeadNodes\":[]}";
  }

  @Override
  protected StorageClusterStatusModel buildTestModel() {
    StorageClusterStatusModel model = new StorageClusterStatusModel();
    model.setRegions(2);
    model.setRequests(0);
    model.setAverageLoad(1.0);
    model.addLiveNode("test1", 1245219839331L, 128, 1024)
      .addRegion(Bytes.toBytes("hbase:root,,0"), 1, 1, 0, 0, 0, 1, 2, 1, 1, 1, 1, 1);
    model.addLiveNode("test2", 1245239331198L, 512, 1024)
      .addRegion(Bytes.toBytes(TableName.META_TABLE_NAME+",,1246000043724"),1, 1, 0, 0, 0,
          1, 2, 1, 1, 1, 1, 1);
    return model;
  }

  @Override
  protected void checkModel(StorageClusterStatusModel model) {
    assertEquals(2, model.getRegions());
    assertEquals(0, model.getRequests());
    assertEquals(1.0, model.getAverageLoad(), 0.0);
    Iterator<StorageClusterStatusModel.Node> nodes =
      model.getLiveNodes().iterator();
    StorageClusterStatusModel.Node node = nodes.next();
    assertEquals("test1", node.getName());
    assertEquals(1245219839331L, node.getStartCode());
    assertEquals(128, node.getHeapSizeMB());
    assertEquals(1024, node.getMaxHeapSizeMB());
    Iterator<StorageClusterStatusModel.Node.Region> regions =
      node.getRegions().iterator();
    StorageClusterStatusModel.Node.Region region = regions.next();
    assertTrue(Bytes.toString(region.getName()).equals(
        "hbase:root,,0"));
    assertEquals(1, region.getStores());
    assertEquals(1, region.getStorefiles());
    assertEquals(0, region.getStorefileSizeMB());
    assertEquals(0, region.getMemStoreSizeMB());
    assertEquals(0, region.getStorefileIndexSizeKB());
    assertEquals(1, region.getReadRequestsCount());
    assertEquals(2, region.getWriteRequestsCount());
    assertEquals(1, region.getRootIndexSizeKB());
    assertEquals(1, region.getTotalStaticIndexSizeKB());
    assertEquals(1, region.getTotalStaticBloomSizeKB());
    assertEquals(1, region.getTotalCompactingKVs());
    assertEquals(1, region.getCurrentCompactedKVs());
    assertFalse(regions.hasNext());
    node = nodes.next();
    assertEquals("test2", node.getName());
    assertEquals(1245239331198L, node.getStartCode());
    assertEquals(512, node.getHeapSizeMB());
    assertEquals(1024, node.getMaxHeapSizeMB());
    regions = node.getRegions().iterator();
    region = regions.next();
    assertEquals(Bytes.toString(region.getName()),
        TableName.META_TABLE_NAME+",,1246000043724");
    assertEquals(1, region.getStores());
    assertEquals(1, region.getStorefiles());
    assertEquals(0, region.getStorefileSizeMB());
    assertEquals(0, region.getMemStoreSizeMB());
    assertEquals(0, region.getStorefileIndexSizeKB());
    assertEquals(1, region.getReadRequestsCount());
    assertEquals(2, region.getWriteRequestsCount());
    assertEquals(1, region.getRootIndexSizeKB());
    assertEquals(1, region.getTotalStaticIndexSizeKB());
    assertEquals(1, region.getTotalStaticBloomSizeKB());
    assertEquals(1, region.getTotalCompactingKVs());
    assertEquals(1, region.getCurrentCompactedKVs());

    assertFalse(regions.hasNext());
    assertFalse(nodes.hasNext());
  }
}

