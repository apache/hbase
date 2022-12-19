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

import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
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
  private static final JsonElement EXCLUDED_NODE = JsonParser.parseString(HConstants.EMPTY_STRING);

  private static JsonElement serializeCatchAll(Operation operation) {
    try {
      return JsonParser.parseString(operation.toJSON());
    } catch (Exception e) {
      LOG.warn("Suppressing exception during OnlineLogRecord serialization with operation {}",
        operation, e);
      return EXCLUDED_NODE;
    }
  }

  private static final Gson INNER_GSON = GsonUtil.createGson().setPrettyPrinting()
    .registerTypeAdapter(Operation.class,
      (JsonSerializer<
        Operation>) (operation, type, jsonSerializationContext) -> serializeCatchAll(operation))
    .registerTypeAdapter(Optional.class,
      (JsonSerializer<Optional<?>>) (optional, type, jsonSerializationContext) -> optional
        .map(jsonSerializationContext::serialize).orElse(EXCLUDED_NODE))
    .create();
  private static final Gson GSON =
    GsonUtil.createGson().setPrettyPrinting().registerTypeAdapter(OnlineLogRecord.class,
      (JsonSerializer<OnlineLogRecord>) (slowLogPayload, type, jsonSerializationContext) -> {
        JsonObject jsonObj = (JsonObject) INNER_GSON.toJsonTree(slowLogPayload);
        if (slowLogPayload.getMultiGetsCount() == 0) {
          jsonObj.remove("multiGetsCount");
        }
        if (slowLogPayload.getMultiMutationsCount() == 0) {
          jsonObj.remove("multiMutationsCount");
        }
        if (slowLogPayload.getMultiServiceCalls() == 0) {
          jsonObj.remove("multiServiceCalls");
        }
        return jsonObj;
      }).create();

  private final long startTime;
  private final int processingTime;
  private final int queueTime;
  private final long responseSize;
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
  private final Optional<List<Operation>> multi;
  private final Optional<Get> get;
  private final Optional<Mutation> mutate;

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
   * If {@value org.apache.hadoop.hbase.HConstants#SLOW_LOG_OPERATION_MESSAGE_PAYLOAD_ENABLED} is
   * enabled then this value may be present and should represent the Scan that produced the given
   * {@link OnlineLogRecord}. This value should only be present if {@link #getMulti()},
   * {@link #getGet()}, and {@link #getMutate()} are empty
   */
  public Optional<Scan> getScan() {
    return scan;
  }

  /**
   * If {@value org.apache.hadoop.hbase.HConstants#SLOW_LOG_OPERATION_MESSAGE_PAYLOAD_ENABLED} is
   * enabled then this value may be present and should represent the MultiRequest that produced the
   * given {@link OnlineLogRecord}. This value should only be present if {@link #getScan},
   * {@link #getGet()}, and {@link #getMutate()} are empty
   */
  public Optional<List<Operation>> getMulti() {
    return multi;
  }

  /**
   * If {@value org.apache.hadoop.hbase.HConstants#SLOW_LOG_OPERATION_MESSAGE_PAYLOAD_ENABLED} is
   * enabled then this value may be present and should represent the Get that produced the given
   * {@link OnlineLogRecord}. This value should only be present if {@link #getScan()},
   * {@link #getMulti()} ()}, and {@link #getMutate()} are empty
   */
  public Optional<Get> getGet() {
    return get;
  }

  /**
   * If {@value org.apache.hadoop.hbase.HConstants#SLOW_LOG_OPERATION_MESSAGE_PAYLOAD_ENABLED} is
   * enabled then this value may be present and should represent the Mutation that produced the
   * given {@link OnlineLogRecord}. This value should only be present if {@link #getScan},
   * {@link #getMulti()} ()}, and {@link #getGet()} ()} are empty
   */
  public Optional<Mutation> getMutate() {
    return mutate;
  }

  OnlineLogRecord(final long startTime, final int processingTime, final int queueTime,
    final long responseSize, final String clientAddress, final String serverClass,
    final String methodName, final String callDetails, final String param, final String regionName,
    final String userName, final int multiGetsCount, final int multiMutationsCount,
    final int multiServiceCalls, final Optional<Scan> scan, final Optional<List<Operation>> multi,
    final Optional<Get> get, final Optional<Mutation> mutate) {
    this.startTime = startTime;
    this.processingTime = processingTime;
    this.queueTime = queueTime;
    this.responseSize = responseSize;
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
    this.multi = multi;
    this.get = get;
    this.mutate = mutate;
  }

  public static class OnlineLogRecordBuilder {
    private long startTime;
    private int processingTime;
    private int queueTime;
    private long responseSize;
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
    private Optional<List<Operation>> multi = Optional.empty();
    private Optional<Get> get = Optional.empty();
    private Optional<Mutation> mutate = Optional.empty();

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

    public OnlineLogRecordBuilder setMulti(List<Operation> multi) {
      this.multi = Optional.of(multi);
      return this;
    }

    public OnlineLogRecordBuilder setGet(Get get) {
      this.get = Optional.of(get);
      return this;
    }

    public OnlineLogRecordBuilder setMutate(Mutation mutate) {
      this.mutate = Optional.of(mutate);
      return this;
    }

    public OnlineLogRecord build() {
      return new OnlineLogRecord(startTime, processingTime, queueTime, responseSize, clientAddress,
        serverClass, methodName, callDetails, param, regionName, userName, multiGetsCount,
        multiMutationsCount, multiServiceCalls, scan, multi, get, mutate);
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
      .append(responseSize, that.responseSize).append(multiGetsCount, that.multiGetsCount)
      .append(multiMutationsCount, that.multiMutationsCount)
      .append(multiServiceCalls, that.multiServiceCalls).append(clientAddress, that.clientAddress)
      .append(serverClass, that.serverClass).append(methodName, that.methodName)
      .append(callDetails, that.callDetails).append(param, that.param)
      .append(regionName, that.regionName).append(userName, that.userName).append(scan, that.scan)
      .append(multi, that.multi).append(get, that.get).append(mutate, that.mutate).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(startTime).append(processingTime).append(queueTime)
      .append(responseSize).append(clientAddress).append(serverClass).append(methodName)
      .append(callDetails).append(param).append(regionName).append(userName).append(multiGetsCount)
      .append(multiMutationsCount).append(multiServiceCalls).append(scan).append(multi).append(get)
      .append(mutate).toHashCode();
  }

  @Override
  public String toJsonPrettyPrint() {
    return GSON.toJson(this);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("startTime", startTime)
      .append("processingTime", processingTime).append("queueTime", queueTime)
      .append("responseSize", responseSize).append("clientAddress", clientAddress)
      .append("serverClass", serverClass).append("methodName", methodName)
      .append("callDetails", callDetails).append("param", param).append("regionName", regionName)
      .append("userName", userName).append("multiGetsCount", multiGetsCount)
      .append("multiMutationsCount", multiMutationsCount)
      .append("multiServiceCalls", multiServiceCalls).append("scan", scan).append("multi", multi)
      .append("get", get).append("mutate", mutate).toString();
  }

}
