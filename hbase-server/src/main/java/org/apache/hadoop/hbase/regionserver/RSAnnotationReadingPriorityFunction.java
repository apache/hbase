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
package org.apache.hadoop.hbase.regionserver;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.AnnotationReadingPriorityFunction;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Priority function specifically for the region server.
 */
@InterfaceAudience.Private
public class RSAnnotationReadingPriorityFunction
  extends AnnotationReadingPriorityFunction<RSRpcServices> {

  private static final Logger LOG =
    LoggerFactory.getLogger(RSAnnotationReadingPriorityFunction.class);

  /** Used to control the scan delay, currently sqrt(numNextCall * weight) */
  public static final String SCAN_VTIME_WEIGHT_CONF_KEY = "hbase.ipc.server.scan.vtime.weight";

  // QOS for internal meta read requests
  public static final int INTERNAL_READ_QOS = 250;

  @SuppressWarnings("unchecked")
  private final Class<? extends Message>[] knownArgumentClasses =
    new Class[] { GetRegionInfoRequest.class, GetStoreFileRequest.class, CloseRegionRequest.class,
      FlushRegionRequest.class, CompactRegionRequest.class, GetRequest.class, MutateRequest.class,
      ScanRequest.class };

  // Some caches for helping performance
  private final Map<String, Class<? extends Message>> argumentToClassMap = new HashMap<>();
  private final Map<String, Map<Class<? extends Message>, Method>> methodMap = new HashMap<>();

  private final float scanVirtualTimeWeight;

  RSAnnotationReadingPriorityFunction(RSRpcServices rpcServices) {
    super(rpcServices);
    if (methodMap.get("getRegion") == null) {
      methodMap.put("hasRegion", new HashMap<>());
      methodMap.put("getRegion", new HashMap<>());
    }
    for (Class<? extends Message> cls : knownArgumentClasses) {
      argumentToClassMap.put(cls.getName(), cls);
      try {
        methodMap.get("hasRegion").put(cls, cls.getDeclaredMethod("hasRegion"));
        methodMap.get("getRegion").put(cls, cls.getDeclaredMethod("getRegion"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    Configuration conf = rpcServices.getConfiguration();
    scanVirtualTimeWeight = conf.getFloat(SCAN_VTIME_WEIGHT_CONF_KEY, 1.0f);
  }

  @Override
  protected int normalizePriority(int priority) {
    return priority;
  }

  @Override
  protected int getBasePriority(RequestHeader header, Message param) {
    // Trust the client-set priorities if set
    if (header.hasPriority()) {
      return header.getPriority();
    }
    if (param instanceof BulkLoadHFileRequest) {
      return HConstants.BULKLOAD_QOS;
    }

    String cls = param.getClass().getName();
    Class<? extends Message> rpcArgClass = argumentToClassMap.get(cls);
    RegionSpecifier regionSpecifier = null;
    // check whether the request has reference to meta region or now.
    try {
      // Check if the param has a region specifier; the pb methods are hasRegion and getRegion if
      // hasRegion returns true. Not all listed methods have region specifier each time. For
      // example, the ScanRequest has it on setup but thereafter relies on the scannerid rather than
      // send the region over every time.
      Method hasRegion = methodMap.get("hasRegion").get(rpcArgClass);
      if (hasRegion != null && (Boolean) hasRegion.invoke(param, (Object[]) null)) {
        Method getRegion = methodMap.get("getRegion").get(rpcArgClass);
        regionSpecifier = (RegionSpecifier) getRegion.invoke(param, (Object[]) null);
        Region region = rpcServices.getRegion(regionSpecifier);
        if (region.getRegionInfo().getTable().isSystemTable()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(
              "High priority because region=" + region.getRegionInfo().getRegionNameAsString());
          }
          return HConstants.SYSTEMTABLE_QOS;
        }
      }
    } catch (Exception ex) {
      // Not good throwing an exception out of here, a runtime anyways. Let the query go into the
      // server and have it throw the exception if still an issue. Just mark it normal priority.
      if (LOG.isTraceEnabled()) LOG.trace("Marking normal priority after getting exception=" + ex);
      return HConstants.NORMAL_QOS;
    }

    if (param instanceof ScanRequest) { // scanner methods...
      ScanRequest request = (ScanRequest) param;
      if (!request.hasScannerId()) {
        return HConstants.NORMAL_QOS;
      }
      RegionScanner scanner = rpcServices.getScanner(request.getScannerId());
      if (scanner != null && scanner.getRegionInfo().getTable().isSystemTable()) {
        if (LOG.isTraceEnabled()) {
          // Scanner requests are small in size so TextFormat version should not overwhelm log.
          LOG.trace("High priority scanner request " + TextFormat.shortDebugString(request));
        }
        return HConstants.SYSTEMTABLE_QOS;
      }
    }

    return HConstants.NORMAL_QOS;
  }

  /**
   * Based on the request content, returns the deadline of the request.
   * @return Deadline of this request. 0 now, otherwise msec of 'delay'
   */
  @Override
  public long getDeadline(RequestHeader header, Message param) {
    if (param instanceof ScanRequest) {
      ScanRequest request = (ScanRequest) param;
      if (!request.hasScannerId()) {
        return 0;
      }

      // get the 'virtual time' of the scanner, and applies sqrt() to get a
      // nice curve for the delay. More a scanner is used the less priority it gets.
      // The weight is used to have more control on the delay.
      long vtime = rpcServices.getScannerVirtualTime(request.getScannerId());
      return Math.round(Math.sqrt(vtime * scanVirtualTimeWeight));
    }
    return 0;
  }
}
