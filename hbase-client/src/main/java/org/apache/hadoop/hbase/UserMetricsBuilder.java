/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

@InterfaceAudience.Private
public final class UserMetricsBuilder {

  public static UserMetrics toUserMetrics(ClusterStatusProtos.UserLoad userLoad) {
    UserMetricsBuilder builder = UserMetricsBuilder.newBuilder(userLoad.getUserName().getBytes());
    userLoad.getClientMetricsList().stream().map(
      clientMetrics -> new ClientMetricsImpl(clientMetrics.getHostName(),
            clientMetrics.getReadRequestsCount(), clientMetrics.getWriteRequestsCount(),
            clientMetrics.getFilteredRequestsCount())).forEach(builder::addClientMetris);
    return builder.build();
  }

  public static ClusterStatusProtos.UserLoad toUserMetrics(UserMetrics userMetrics) {
    ClusterStatusProtos.UserLoad.Builder builder =
        ClusterStatusProtos.UserLoad.newBuilder().setUserName(userMetrics.getNameAsString());
    userMetrics.getClientMetrics().values().stream().map(
      clientMetrics -> ClusterStatusProtos.ClientMetrics.newBuilder()
            .setHostName(clientMetrics.getHostName())
            .setWriteRequestsCount(clientMetrics.getWriteRequestsCount())
            .setReadRequestsCount(clientMetrics.getReadRequestsCount())
            .setFilteredRequestsCount(clientMetrics.getFilteredReadRequestsCount()).build())
        .forEach(builder::addClientMetrics);
    return builder.build();
  }

  public static UserMetricsBuilder newBuilder(byte[] name) {
    return new UserMetricsBuilder(name);
  }


  private final byte[] name;
  private Map<String, UserMetrics.ClientMetrics> clientMetricsMap = new HashMap<>();
  private UserMetricsBuilder(byte[] name) {
    this.name = name;
  }

  public UserMetricsBuilder addClientMetris(UserMetrics.ClientMetrics clientMetrics) {
    clientMetricsMap.put(clientMetrics.getHostName(), clientMetrics);
    return this;
  }

  public UserMetrics build() {
    return new UserMetricsImpl(name, clientMetricsMap);
  }

  public static class ClientMetricsImpl implements UserMetrics.ClientMetrics {
    private final long filteredReadRequestsCount;
    private final String hostName;
    private final long readRequestCount;
    private final long writeRequestCount;

    public ClientMetricsImpl(String hostName, long readRequest, long writeRequest,
        long filteredReadRequestsCount) {
      this.hostName = hostName;
      this.readRequestCount = readRequest;
      this.writeRequestCount = writeRequest;
      this.filteredReadRequestsCount = filteredReadRequestsCount;
    }

    @Override public String getHostName() {
      return hostName;
    }

    @Override public long getReadRequestsCount() {
      return readRequestCount;
    }

    @Override public long getWriteRequestsCount() {
      return writeRequestCount;
    }

    @Override public long getFilteredReadRequestsCount() {
      return filteredReadRequestsCount;
    }
  }

  private static class UserMetricsImpl implements UserMetrics {
    private final byte[] name;
    private final Map<String, ClientMetrics> clientMetricsMap;

    UserMetricsImpl(byte[] name, Map<String, ClientMetrics> clientMetricsMap) {
      this.name = Preconditions.checkNotNull(name);
      this.clientMetricsMap = clientMetricsMap;
    }

    @Override public byte[] getUserName() {
      return name;
    }

    @Override public long getReadRequestCount() {
      return clientMetricsMap.values().stream().map(c -> c.getReadRequestsCount())
          .reduce(0L, Long::sum);
    }

    @Override public long getWriteRequestCount() {
      return clientMetricsMap.values().stream().map(c -> c.getWriteRequestsCount())
          .reduce(0L, Long::sum);
    }

    @Override public Map<String, ClientMetrics> getClientMetrics() {
      return this.clientMetricsMap;
    }

    @Override public long getFilteredReadRequests() {
      return clientMetricsMap.values().stream().map(c -> c.getFilteredReadRequestsCount())
          .reduce(0L, Long::sum);
    }

    @Override
    public String toString() {
      StringBuilder sb = Strings
          .appendKeyValue(new StringBuilder(), "readRequestCount", this.getReadRequestCount());
      Strings.appendKeyValue(sb, "writeRequestCount", this.getWriteRequestCount());
      Strings.appendKeyValue(sb, "filteredReadRequestCount", this.getFilteredReadRequests());
      return sb.toString();
    }
  }

}
