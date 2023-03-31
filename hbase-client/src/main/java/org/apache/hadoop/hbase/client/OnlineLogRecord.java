/*
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

import java.io.IOException;
import java.util.Optional;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParser;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializer;

/**
 * Slow/Large Log payload for hbase-client, to be used by Admin API get_slow_responses and
 * get_large_responses
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
final public class OnlineLogRecord extends LogEntry {

  private static final Logger LOG = LoggerFactory.getLogger(OnlineLogRecord.class.getName());

  // used to convert object to pretty printed format
  // used by toJsonPrettyPrint()
  private static final Gson GSON =
    GsonUtil.createGson().setPrettyPrinting().registerTypeAdapter(OnlineLogRecord.class,
      (JsonSerializer<OnlineLogRecord>) (slowLogPayload, type, jsonSerializationContext) -> {
        Gson gson = new Gson();
        JsonObject jsonObj = (JsonObject) gson.toJsonTree(slowLogPayload);
        if (slowLogPayload.getMultiGetsCount() == 0) {
          jsonObj.remove("multiGetsCount");
        }
        if (slowLogPayload.getMultiMutationsCount() == 0) {
          jsonObj.remove("multiMutationsCount");
        }
        if (slowLogPayload.getMultiServiceCalls() == 0) {
          jsonObj.remove("multiServiceCalls");
        }
        if (slowLogPayload.getScan().isPresent()) {
          try {
            jsonObj.add("scan", JsonParser.parseString(slowLogPayload.getScan().get().toJSON()));
          } catch (IOException e) {
            LOG.warn("Failed to serialize scan {}", slowLogPayload.getScan().get(), e);
          }
        } else {
          jsonObj.remove("scan");
        }
        return jsonObj;
      }).create();

  private final long startTime;
  private final int processingTime;
  private final int queueTime;
  private final long responseSize;
  private final long blockBytesScanned;
  private final String clientAddress;
  private final String serverClass;
  private final String methodName;
  private final String callDetails;
  private final String param;
  // we don't want to serialize region name, it is just for the filter purpose
  // hence avoiding deserialization
  private final transient String regionName;
  private final String userName;
  private final int multiGetsCount;
  private final int multiMutationsCount;
  private final int multiServiceCalls;
  private final Optional<Scan> scan;

  public long getStartTime() {
    return startTime;
  }

  public int getProcessingTime() {
    return processingTime;
  }

  public int getQueueTime() {
    return queueTime;
  }

  public long getResponseSize() {
    return responseSize;
  }

  /**
   * Return the amount of block bytes scanned to retrieve the response cells.
   */
  public long getBlockBytesScanned() {
    return blockBytesScanned;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public String getServerClass() {
    return serverClass;
  }

  public String getMethodName() {
    return methodName;
  }

  public String getCallDetails() {
    return callDetails;
  }

  public String getParam() {
    return param;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getUserName() {
    return userName;
  }

  public int getMultiGetsCount() {
    return multiGetsCount;
  }

  public int getMultiMutationsCount() {
    return multiMutationsCount;
  }

  public int getMultiServiceCalls() {
    return multiServiceCalls;
  }

  /**
   * If {@value org.apache.hadoop.hbase.HConstants#SLOW_LOG_SCAN_PAYLOAD_ENABLED} is enabled then
   * this value may be present and should represent the Scan that produced the given
   * {@link OnlineLogRecord}
   */
  public Optional<Scan> getScan() {
    return scan;
  }

  protected OnlineLogRecord(final long startTime, final int processingTime, final int queueTime,
    final long responseSize, final long blockBytesScanned, final String clientAddress,
    final String serverClass, final String methodName, final String callDetails, final String param,
    final String regionName, final String userName, final int multiGetsCount,
    final int multiMutationsCount, final int multiServiceCalls, final Optional<Scan> scan) {
    this.startTime = startTime;
    this.processingTime = processingTime;
    this.queueTime = queueTime;
    this.responseSize = responseSize;
    this.blockBytesScanned = blockBytesScanned;
    this.clientAddress = clientAddress;
    this.serverClass = serverClass;
    this.methodName = methodName;
    this.callDetails = callDetails;
    this.param = param;
    this.regionName = regionName;
    this.userName = userName;
    this.multiGetsCount = multiGetsCount;
    this.multiMutationsCount = multiMutationsCount;
    this.multiServiceCalls = multiServiceCalls;
    this.scan = scan;
  }

  public static class OnlineLogRecordBuilder {
    private long startTime;
    private int processingTime;
    private int queueTime;
    private long responseSize;
    private long blockBytesScanned;
    private String clientAddress;
    private String serverClass;
    private String methodName;
    private String callDetails;
    private String param;
    private String regionName;
    private String userName;
    private int multiGetsCount;
    private int multiMutationsCount;
    private int multiServiceCalls;
    private Optional<Scan> scan = Optional.empty();

    public OnlineLogRecordBuilder setStartTime(long startTime) {
      this.startTime = startTime;
      return this;
    }

    public OnlineLogRecordBuilder setProcessingTime(int processingTime) {
      this.processingTime = processingTime;
      return this;
    }

    public OnlineLogRecordBuilder setQueueTime(int queueTime) {
      this.queueTime = queueTime;
      return this;
    }

    public OnlineLogRecordBuilder setResponseSize(long responseSize) {
      this.responseSize = responseSize;
      return this;
    }

    /**
     * Sets the amount of block bytes scanned to retrieve the response cells.
     */
    public OnlineLogRecordBuilder setBlockBytesScanned(long blockBytesScanned) {
      this.blockBytesScanned = blockBytesScanned;
      return this;
    }

    public OnlineLogRecordBuilder setClientAddress(String clientAddress) {
      this.clientAddress = clientAddress;
      return this;
    }

    public OnlineLogRecordBuilder setServerClass(String serverClass) {
      this.serverClass = serverClass;
      return this;
    }

    public OnlineLogRecordBuilder setMethodName(String methodName) {
      this.methodName = methodName;
      return this;
    }

    public OnlineLogRecordBuilder setCallDetails(String callDetails) {
      this.callDetails = callDetails;
      return this;
    }

    public OnlineLogRecordBuilder setParam(String param) {
      this.param = param;
      return this;
    }

    public OnlineLogRecordBuilder setRegionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    public OnlineLogRecordBuilder setUserName(String userName) {
      this.userName = userName;
      return this;
    }

    public OnlineLogRecordBuilder setMultiGetsCount(int multiGetsCount) {
      this.multiGetsCount = multiGetsCount;
      return this;
    }

    public OnlineLogRecordBuilder setMultiMutationsCount(int multiMutationsCount) {
      this.multiMutationsCount = multiMutationsCount;
      return this;
    }

    public OnlineLogRecordBuilder setMultiServiceCalls(int multiServiceCalls) {
      this.multiServiceCalls = multiServiceCalls;
      return this;
    }

    public OnlineLogRecordBuilder setScan(Scan scan) {
      this.scan = Optional.of(scan);
      return this;
    }

    public OnlineLogRecord build() {
      return new OnlineLogRecord(startTime, processingTime, queueTime, responseSize,
        blockBytesScanned, clientAddress, serverClass, methodName, callDetails, param, regionName,
        userName, multiGetsCount, multiMutationsCount, multiServiceCalls, scan);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OnlineLogRecord that = (OnlineLogRecord) o;

    return new EqualsBuilder().append(startTime, that.startTime)
      .append(processingTime, that.processingTime).append(queueTime, that.queueTime)
      .append(responseSize, that.responseSize).append(blockBytesScanned, that.blockBytesScanned)
      .append(multiGetsCount, that.multiGetsCount)
      .append(multiMutationsCount, that.multiMutationsCount)
      .append(multiServiceCalls, that.multiServiceCalls).append(clientAddress, that.clientAddress)
      .append(serverClass, that.serverClass).append(methodName, that.methodName)
      .append(callDetails, that.callDetails).append(param, that.param)
      .append(regionName, that.regionName).append(userName, that.userName).append(scan, that.scan)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(startTime).append(processingTime).append(queueTime)
      .append(responseSize).append(blockBytesScanned).append(clientAddress).append(serverClass)
      .append(methodName).append(callDetails).append(param).append(regionName).append(userName)
      .append(multiGetsCount).append(multiMutationsCount).append(multiServiceCalls).append(scan)
      .toHashCode();
  }

  @Override
  public String toJsonPrettyPrint() {
    return GSON.toJson(this);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("startTime", startTime)
      .append("processingTime", processingTime).append("queueTime", queueTime)
      .append("responseSize", responseSize).append("blockBytesScanned", blockBytesScanned)
      .append("clientAddress", clientAddress).append("serverClass", serverClass)
      .append("methodName", methodName).append("callDetails", callDetails).append("param", param)
      .append("regionName", regionName).append("userName", userName)
      .append("multiGetsCount", multiGetsCount).append("multiMutationsCount", multiMutationsCount)
      .append("multiServiceCalls", multiServiceCalls).append("scan", scan).toString();
  }

}
