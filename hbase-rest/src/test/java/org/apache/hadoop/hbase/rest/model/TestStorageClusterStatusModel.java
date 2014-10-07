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

import java.util.Iterator;

import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.experimental.categories.Category;

@Category({RestTests.class, SmallTests.class})
public class TestStorageClusterStatusModel extends TestModelBase<StorageClusterStatusModel> {

  public TestStorageClusterStatusModel() throws Exception {
    super(StorageClusterStatusModel.class);

    AS_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
      "<ClusterStatus averageLoad=\"1.0\" regions=\"2\" requests=\"0\">" +
      "<DeadNodes/><LiveNodes>" +
      "<Node heapSizeMB=\"128\" maxHeapSizeMB=\"1024\" name=\"test1\" requests=\"0\" startCode=\"1245219839331\">" +
      "<Region currentCompactedKVs=\"1\" memstoreSizeMB=\"0\" name=\"aGJhc2U6cm9vdCwsMA==\" readRequestsCount=\"1\" " +
      "rootIndexSizeKB=\"1\" storefileIndexSizeMB=\"0\" storefileSizeMB=\"0\" storefiles=\"1\" stores=\"1\" " +
      "totalCompactingKVs=\"1\" totalStaticBloomSizeKB=\"1\" totalStaticIndexSizeKB=\"1\" writeRequestsCount=\"2\"/>" +
      "</Node>" +
      "<Node heapSizeMB=\"512\" maxHeapSizeMB=\"1024\" name=\"test2\" requests=\"0\" startCode=\"1245239331198\">" +
      "<Region currentCompactedKVs=\"1\" memstoreSizeMB=\"0\" name=\"aGJhc2U6bWV0YSwsMTI0NjAwMDA0MzcyNA==\" " +
      "readRequestsCount=\"1\" rootIndexSizeKB=\"1\" storefileIndexSizeMB=\"0\" storefileSizeMB=\"0\" " +
      "storefiles=\"1\" stores=\"1\" totalCompactingKVs=\"1\" totalStaticBloomSizeKB=\"1\" " +
      "totalStaticIndexSizeKB=\"1\" writeRequestsCount=\"2\"/></Node></LiveNodes></ClusterStatus>";

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
          "\"storefileSizeMB\":0,\"memstoreSizeMB\":0,\"storefileIndexSizeMB\":0," +
          "\"readRequestsCount\":1,\"writeRequestsCount\":2,\"rootIndexSizeKB\":1," +
          "\"totalStaticIndexSizeKB\":1,\"totalStaticBloomSizeKB\":1,\"totalCompactingKVs\":1," +
          "\"currentCompactedKVs\":1}],\"requests\":0,\"startCode\":1245219839331," +
          "\"heapSizeMB\":128,\"maxHeapSizeMB\":1024},{\"name\":\"test2\"," +
          "\"Region\":[{\"name\":\"aGJhc2U6bWV0YSwsMTI0NjAwMDA0MzcyNA==\",\"stores\":1," +
          "\"storefiles\":1,\"storefileSizeMB\":0,\"memstoreSizeMB\":0,\"storefileIndexSizeMB\":0," +
          "\"readRequestsCount\":1,\"writeRequestsCount\":2,\"rootIndexSizeKB\":1," +
          "\"totalStaticIndexSizeKB\":1,\"totalStaticBloomSizeKB\":1,\"totalCompactingKVs\":1," +
          "\"currentCompactedKVs\":1}],\"requests\":0,\"startCode\":1245239331198," +
          "\"heapSizeMB\":512,\"maxHeapSizeMB\":1024}],\"DeadNodes\":[]}";
  }

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

  protected void checkModel(StorageClusterStatusModel model) {
    assertEquals(model.getRegions(), 2);
    assertEquals(model.getRequests(), 0);
    assertEquals(model.getAverageLoad(), 1.0);
    Iterator<StorageClusterStatusModel.Node> nodes =
      model.getLiveNodes().iterator();
    StorageClusterStatusModel.Node node = nodes.next();
    assertEquals(node.getName(), "test1");
    assertEquals(node.getStartCode(), 1245219839331L);
    assertEquals(node.getHeapSizeMB(), 128);
    assertEquals(node.getMaxHeapSizeMB(), 1024);
    Iterator<StorageClusterStatusModel.Node.Region> regions = 
      node.getRegions().iterator();
    StorageClusterStatusModel.Node.Region region = regions.next();
    assertTrue(Bytes.toString(region.getName()).equals(
        "hbase:root,,0"));
    assertEquals(region.getStores(), 1);
    assertEquals(region.getStorefiles(), 1);
    assertEquals(region.getStorefileSizeMB(), 0);
    assertEquals(region.getMemstoreSizeMB(), 0);
    assertEquals(region.getStorefileIndexSizeMB(), 0);
    assertEquals(region.getReadRequestsCount(), 1);
    assertEquals(region.getWriteRequestsCount(), 2);
    assertEquals(region.getRootIndexSizeKB(), 1);
    assertEquals(region.getTotalStaticIndexSizeKB(), 1);
    assertEquals(region.getTotalStaticBloomSizeKB(), 1);
    assertEquals(region.getTotalCompactingKVs(), 1);
    assertEquals(region.getCurrentCompactedKVs(), 1);
    assertFalse(regions.hasNext());
    node = nodes.next();
    assertEquals(node.getName(), "test2");
    assertEquals(node.getStartCode(), 1245239331198L);
    assertEquals(node.getHeapSizeMB(), 512);
    assertEquals(node.getMaxHeapSizeMB(), 1024);
    regions = node.getRegions().iterator();
    region = regions.next();
    assertEquals(Bytes.toString(region.getName()),
        TableName.META_TABLE_NAME+",,1246000043724");
    assertEquals(region.getStores(), 1);
    assertEquals(region.getStorefiles(), 1);
    assertEquals(region.getStorefileSizeMB(), 0);
    assertEquals(region.getMemstoreSizeMB(), 0);
    assertEquals(region.getStorefileIndexSizeMB(), 0);
    assertEquals(region.getReadRequestsCount(), 1);
    assertEquals(region.getWriteRequestsCount(), 2);
    assertEquals(region.getRootIndexSizeKB(), 1);
    assertEquals(region.getTotalStaticIndexSizeKB(), 1);
    assertEquals(region.getTotalStaticBloomSizeKB(), 1);
    assertEquals(region.getTotalCompactingKVs(), 1);
    assertEquals(region.getCurrentCompactedKVs(), 1);
    
    assertFalse(regions.hasNext());
    assertFalse(nodes.hasNext());
  }
}

