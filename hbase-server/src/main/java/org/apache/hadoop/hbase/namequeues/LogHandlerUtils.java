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

package org.apache.hadoop.hbase.namequeues;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Event Handler utility class
 */
@InterfaceAudience.Private
public class LogHandlerUtils {

  private static int getTotalFiltersCount(AdminProtos.SlowLogResponseRequest request) {
    int totalFilters = 0;
    if (StringUtils.isNotEmpty(request.getRegionName())) {
      totalFilters++;
    }
    if (StringUtils.isNotEmpty(request.getTableName())) {
      totalFilters++;
    }
    if (StringUtils.isNotEmpty(request.getClientAddress())) {
      totalFilters++;
    }
    if (StringUtils.isNotEmpty(request.getUserName())) {
      totalFilters++;
    }
    return totalFilters;
  }

  private static List<TooSlowLog.SlowLogPayload> filterLogs(
      AdminProtos.SlowLogResponseRequest request,
      List<TooSlowLog.SlowLogPayload> slowLogPayloadList, int totalFilters) {
    List<TooSlowLog.SlowLogPayload> filteredSlowLogPayloads = new ArrayList<>();
    final String regionName =
      StringUtils.isNotEmpty(request.getRegionName()) ? request.getRegionName() : null;
    final String tableName =
      StringUtils.isNotEmpty(request.getTableName()) ? request.getTableName() : null;
    final String clientAddress =
      StringUtils.isNotEmpty(request.getClientAddress()) ? request.getClientAddress() : null;
    final String userName =
      StringUtils.isNotEmpty(request.getUserName()) ? request.getUserName() : null;
    for (TooSlowLog.SlowLogPayload slowLogPayload : slowLogPayloadList) {
      int totalFilterMatches = 0;
      if (slowLogPayload.getRegionName().equals(regionName)) {
        totalFilterMatches++;
      }
      if (tableName != null && slowLogPayload.getRegionName().startsWith(tableName)) {
        totalFilterMatches++;
      }
      if (slowLogPayload.getClientAddress().equals(clientAddress)) {
        totalFilterMatches++;
      }
      if (slowLogPayload.getUserName().equals(userName)) {
        totalFilterMatches++;
      }
      if (request.hasFilterByOperator() && request.getFilterByOperator()
        .equals(AdminProtos.SlowLogResponseRequest.FilterByOperator.AND)) {
        // Filter by AND operator
        if (totalFilterMatches == totalFilters) {
          filteredSlowLogPayloads.add(slowLogPayload);
        }
      } else {
        // Filter by OR operator
        if (totalFilterMatches > 0) {
          filteredSlowLogPayloads.add(slowLogPayload);
        }
      }
    }
    return filteredSlowLogPayloads;
  }

  public static List<TooSlowLog.SlowLogPayload> getFilteredLogs(
      AdminProtos.SlowLogResponseRequest request, List<TooSlowLog.SlowLogPayload> logPayloadList) {
    int totalFilters = getTotalFiltersCount(request);
    if (totalFilters > 0) {
      logPayloadList = filterLogs(request, logPayloadList, totalFilters);
    }
    int limit = Math.min(request.getLimit(), logPayloadList.size());
    return logPayloadList.subList(0, limit);
  }

}
