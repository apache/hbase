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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashMap;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.FileBasedStoreFileCleanerStatus;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

@XmlRootElement(name = "ClusterStatus") @XmlAccessorType(XmlAccessType.FIELD)
@InterfaceAudience.Private public class FileBasedStoreFileCleanerStatusesModel
  implements Serializable, ProtobufMessageHandler {
  private static final long serialVersionUID = 1L;

  @JsonProperty("FileBasedStoreFileCleanerStatuses")
  private HashMap<String, FileBasedStoreFileCleanerStatusModel> fileBasedFileStoreCleanerStatuses =
    new HashMap<>();

  public FileBasedStoreFileCleanerStatusesModel() {
  }

  public FileBasedStoreFileCleanerStatusesModel(Admin admin) throws IOException {
    ClusterMetrics metrics =
      admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.FILEBASED_STORAGE_CLEANER));
    fileBasedFileStoreCleanerStatuses = new HashMap<>();
    metrics.getFileBasedStoreFileCleanerStatus().forEach(
      (sn, status) -> fileBasedFileStoreCleanerStatuses.put(sn,
        new FileBasedStoreFileCleanerStatusModel(status.getLastRuntime(), status.getMinRuntime(),
          status.getMaxRuntime(), status.getDeletedFiles(), status.getFailedDeletes(),
          status.getRuns())));
  }

  @Override public byte[] createProtobufOutput() {
    ClusterMetricsBuilder builder = ClusterMetricsBuilder.newBuilder();
    HashMap<String, FileBasedStoreFileCleanerStatus> statuses = new HashMap<>();
    fileBasedFileStoreCleanerStatuses.forEach(
      (sn, status) -> statuses.put(sn,
        new FileBasedStoreFileCleanerStatus(status.getLastRuntime(), status.getMinRuntime(),
          status.getMaxRuntime(), status.getDeletedFiles(), status.getFailedDeletes(),
          status.getRuns())));
    builder.setFileBasedStoreFileCleanerStatus(statuses);
    return ClusterMetricsBuilder.toClusterStatus(builder.build()).toByteArray();
  }

  @Override public ProtobufMessageHandler getObjectFromMessage(byte[] message) throws IOException {
    ClusterStatusProtos.ClusterStatus.Builder builder =
      ClusterStatusProtos.ClusterStatus.newBuilder();
    builder.mergeFrom(message);
    HashMap<String, FileBasedStoreFileCleanerStatus> statuses =
      (HashMap<String, FileBasedStoreFileCleanerStatus>) ProtobufUtil.toFBSFCleanerStatusMap(
        builder.getFileBasedStoreFileCleanerStatusMap());
    fileBasedFileStoreCleanerStatuses = new HashMap<>();
    statuses.forEach(
      (sn, status) -> fileBasedFileStoreCleanerStatuses.put(sn,
        new FileBasedStoreFileCleanerStatusModel(status.getLastRuntime(), status.getMinRuntime(),
          status.getMaxRuntime(), status.getDeletedFiles(), status.getFailedDeletes(),
          status.getRuns())));
    return this;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileBasedStoreFileCleanerStatusesModel{ fileBasedFileStoreCleanerStatuses=");
    fileBasedFileStoreCleanerStatuses.forEach(
      (sn, status) -> sb.append(" { " + sn + ": " + status.toString() + " }"));
    sb.append("}");
    return sb.toString();
  }

  public HashMap<String, FileBasedStoreFileCleanerStatusModel> getFileBasedFileStoreCleanerStatuses() {
    return fileBasedFileStoreCleanerStatuses;
  }
}
