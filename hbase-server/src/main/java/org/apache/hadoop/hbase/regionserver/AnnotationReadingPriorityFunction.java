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
package org.apache.hadoop.hbase.regionserver;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;

/**
 * Reads special method annotations and table names to figure a priority for use by QoS facility in
 * ipc; e.g: rpcs to hbase:meta get priority.
 */
// TODO: Remove.  This is doing way too much work just to figure a priority.  Do as Elliott
// suggests and just have the client specify a priority.

//The logic for figuring out high priority RPCs is as follows:
//1. if the method is annotated with a QosPriority of QOS_HIGH,
//   that is honored
//2. parse out the protobuf message and see if the request is for meta
//   region, and if so, treat it as a high priority RPC
//Some optimizations for (2) are done here -
//Clients send the argument classname as part of making the RPC. The server
//decides whether to deserialize the proto argument message based on the
//pre-established set of argument classes (knownArgumentClasses below).
//This prevents the server from having to deserialize all proto argument
//messages prematurely.
//All the argument classes declare a 'getRegion' method that returns a
//RegionSpecifier object. Methods can be invoked on the returned object
//to figure out whether it is a meta region or not.
@InterfaceAudience.Private
public class AnnotationReadingPriorityFunction implements PriorityFunction {
  private static final Log LOG =
    LogFactory.getLog(AnnotationReadingPriorityFunction.class.getName());

  /** Used to control the scan delay, currently sqrt(numNextCall * weight) */
  public static final String SCAN_VTIME_WEIGHT_CONF_KEY = "hbase.ipc.server.scan.vtime.weight";

  protected final Map<String, Integer> annotatedQos;
  //We need to mock the regionserver instance for some unit tests (set via
  //setRegionServer method.
  private RSRpcServices rpcServices;
  @SuppressWarnings("unchecked")
  private final Class<? extends Message>[] knownArgumentClasses = new Class[]{
      GetRegionInfoRequest.class,
      GetStoreFileRequest.class,
      CloseRegionRequest.class,
      FlushRegionRequest.class,
      SplitRegionRequest.class,
      CompactRegionRequest.class,
      GetRequest.class,
      MutateRequest.class,
      ScanRequest.class
  };

  // Some caches for helping performance
  private final Map<String, Class<? extends Message>> argumentToClassMap =
    new HashMap<String, Class<? extends Message>>();
  private final Map<String, Map<Class<? extends Message>, Method>> methodMap =
    new HashMap<String, Map<Class<? extends Message>, Method>>();

  private final float scanVirtualTimeWeight;

  /**
   * Calls {@link #AnnotationReadingPriorityFunction(RSRpcServices, Class)} using the result of
   * {@code rpcServices#getClass()}
   *
   * @param rpcServices
   *          The RPC server implementation
   */
  public AnnotationReadingPriorityFunction(final RSRpcServices rpcServices) {
    this(rpcServices, rpcServices.getClass());
  }

  /**
   * Constructs the priority function given the RPC server implementation and the annotations on the
   * methods in the provided {@code clz}.
   *
   * @param rpcServices
   *          The RPC server implementation
   * @param clz
   *          The concrete RPC server implementation's class
   */
  public AnnotationReadingPriorityFunction(final RSRpcServices rpcServices,
      Class<? extends RSRpcServices> clz) {
    Map<String,Integer> qosMap = new HashMap<String,Integer>();
    for (Method m : clz.getMethods()) {
      QosPriority p = m.getAnnotation(QosPriority.class);
      if (p != null) {
        // Since we protobuf'd, and then subsequently, when we went with pb style, method names
        // are capitalized.  This meant that this brittle compare of method names gotten by
        // reflection no longer matched the method names coming in over pb.  TODO: Get rid of this
        // check.  For now, workaround is to capitalize the names we got from reflection so they
        // have chance of matching the pb ones.
        String capitalizedMethodName = capitalize(m.getName());
        qosMap.put(capitalizedMethodName, p.priority());
      }
    }
    this.rpcServices = rpcServices;
    this.annotatedQos = qosMap;
    if (methodMap.get("getRegion") == null) {
      methodMap.put("hasRegion", new HashMap<Class<? extends Message>, Method>());
      methodMap.put("getRegion", new HashMap<Class<? extends Message>, Method>());
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

  private String capitalize(final String s) {
    StringBuilder strBuilder = new StringBuilder(s);
    strBuilder.setCharAt(0, Character.toUpperCase(strBuilder.charAt(0)));
    return strBuilder.toString();
  }

  /**
   * Returns a 'priority' based on the request type.
   *
   * Currently the returned priority is used for queue selection.
   * See the SimpleRpcScheduler as example. It maintains a queue per 'priory type'
   * HIGH_QOS (meta requests), REPLICATION_QOS (replication requests),
   * NORMAL_QOS (user requests).
   */
  @Override
  public int getPriority(RequestHeader header, Message param, User user) {
    int priorityByAnnotation = getAnnotatedPriority(header);

    if (priorityByAnnotation >= 0) {
      return priorityByAnnotation;
    }

    // all requests executed by super users have high QoS
    try {
      if (Superusers.isSuperUser(user)) {
        return HConstants.ADMIN_QOS;
      }
    } catch (IllegalStateException ex) {
      // Not good throwing an exception out of here, a runtime anyways.  Let the query go into the
      // server and have it throw the exception if still an issue.  Just mark it normal priority.
      if (LOG.isTraceEnabled()) LOG.trace("Marking normal priority after getting exception=" + ex);
      return HConstants.NORMAL_QOS;
    }

    return getBasePriority(header, param);
  }

  /**
   * See if the method has an annotation.
   * @param header
   * @return Return the priority from the annotation. If there isn't
   * an annotation, this returns something below zero.
   */
  protected int getAnnotatedPriority(RequestHeader header) {
    String methodName = header.getMethodName();
    Integer priorityByAnnotation = annotatedQos.get(methodName);
    if (priorityByAnnotation != null) {
      return priorityByAnnotation;
    }
    return -1;
  }

  /**
   * Get the priority for a given request from the header and the param
   * This doesn't consider which user is sending the request at all.
   * This doesn't consider annotations
   */
  protected int getBasePriority(RequestHeader header, Message param) {
    if (param == null) {
      return HConstants.NORMAL_QOS;
    }

    // Trust the client-set priorities if set
    if (header.hasPriority()) {
      return header.getPriority();
    }

    String cls = param.getClass().getName();
    Class<? extends Message> rpcArgClass = argumentToClassMap.get(cls);
    RegionSpecifier regionSpecifier = null;
    //check whether the request has reference to meta region or now.
    try {
      // Check if the param has a region specifier; the pb methods are hasRegion and getRegion if
      // hasRegion returns true.  Not all listed methods have region specifier each time.  For
      // example, the ScanRequest has it on setup but thereafter relies on the scannerid rather than
      // send the region over every time.
      Method hasRegion = methodMap.get("hasRegion").get(rpcArgClass);
      if (hasRegion != null && (Boolean)hasRegion.invoke(param, (Object[])null)) {
        Method getRegion = methodMap.get("getRegion").get(rpcArgClass);
        regionSpecifier = (RegionSpecifier)getRegion.invoke(param, (Object[])null);
        Region region = rpcServices.getRegion(regionSpecifier);
        if (region.getRegionInfo().isSystemTable()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("High priority because region=" +
              region.getRegionInfo().getRegionNameAsString());
          }
          return HConstants.SYSTEMTABLE_QOS;
        }
      }
    } catch (Exception ex) {
      // Not good throwing an exception out of here, a runtime anyways.  Let the query go into the
      // server and have it throw the exception if still an issue.  Just mark it normal priority.
      if (LOG.isTraceEnabled()) LOG.trace("Marking normal priority after getting exception=" + ex);
      return HConstants.NORMAL_QOS;
    }

    if (param instanceof ScanRequest) { // scanner methods...
      ScanRequest request = (ScanRequest)param;
      if (!request.hasScannerId()) {
        return HConstants.NORMAL_QOS;
      }
      RegionScanner scanner = rpcServices.getScanner(request.getScannerId());
      if (scanner != null && scanner.getRegionInfo().isSystemTable()) {
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
   *
   * @param header
   * @param param
   * @return Deadline of this request. 0 now, otherwise msec of 'delay'
   */
  @Override
  public long getDeadline(RequestHeader header, Message param) {
    if (param instanceof ScanRequest) {
      ScanRequest request = (ScanRequest)param;
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

  @VisibleForTesting
  void setRegionServer(final HRegionServer hrs) {
    this.rpcServices = hrs.getRSRpcServices();
  }
}
