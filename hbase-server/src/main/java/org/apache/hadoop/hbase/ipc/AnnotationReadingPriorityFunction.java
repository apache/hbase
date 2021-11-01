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
package org.apache.hadoop.hbase.ipc;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseRpcServicesBase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Reads special method annotations and table names to figure a priority for use by QoS facility in
 * ipc; e.g: rpcs to hbase:meta get priority.
 */
// TODO: Remove. This is doing way too much work just to figure a priority. Do as Elliott
// suggests and just have the client specify a priority.

// The logic for figuring out high priority RPCs is as follows:
// 1. if the method is annotated with a QosPriority of QOS_HIGH,
// that is honored
// 2. parse out the protobuf message and see if the request is for meta
// region, and if so, treat it as a high priority RPC
// Some optimizations for (2) are done in the sub classes -
// Clients send the argument classname as part of making the RPC. The server
// decides whether to deserialize the proto argument message based on the
// pre-established set of argument classes (knownArgumentClasses below).
// This prevents the server from having to deserialize all proto argument
// messages prematurely.
// All the argument classes declare a 'getRegion' method that returns a
// RegionSpecifier object. Methods can be invoked on the returned object
// to figure out whether it is a meta region or not.
@InterfaceAudience.Private
public abstract class AnnotationReadingPriorityFunction<T extends HBaseRpcServicesBase<?>>
  implements PriorityFunction {

  protected final Map<String, Integer> annotatedQos;
  // We need to mock the regionserver instance for some unit tests (set via
  // setRegionServer method.
  protected final T rpcServices;

  /**
   * Constructs the priority function given the RPC server implementation and the annotations on the
   * methods.
   * @param rpcServices The RPC server implementation
   */
  public AnnotationReadingPriorityFunction(final T rpcServices) {
    Map<String, Integer> qosMap = new HashMap<>();
    for (Method m : rpcServices.getClass().getMethods()) {
      QosPriority p = m.getAnnotation(QosPriority.class);
      if (p != null) {
        // Since we protobuf'd, and then subsequently, when we went with pb style, method names
        // are capitalized. This meant that this brittle compare of method names gotten by
        // reflection no longer matched the method names coming in over pb.
        // TODO: Get rid of this check. For now, workaround is to capitalize the names we got from
        // reflection so they have chance of matching the pb ones.
        String capitalizedMethodName = StringUtils.capitalize(m.getName());
        qosMap.put(capitalizedMethodName, p.priority());
      }
    }
    this.rpcServices = rpcServices;
    this.annotatedQos = qosMap;
  }

  /**
   * Returns a 'priority' based on the request type.
   * <p/>
   * Currently the returned priority is used for queue selection.
   * <p/>
   * See the {@code SimpleRpcScheduler} as example. It maintains a queue per 'priority type':
   * <ul>
   * <li>HIGH_QOS (meta requests)</li>
   * <li>REPLICATION_QOS (replication requests)</li>
   * <li>NORMAL_QOS (user requests).</li>
   * </ul>
   */
  @Override
  public int getPriority(RequestHeader header, Message param, User user) {
    int priorityByAnnotation = getAnnotatedPriority(header);

    if (priorityByAnnotation >= 0) {
      return priorityByAnnotation;
    }
    if (param == null) {
      return HConstants.NORMAL_QOS;
    }
    return getBasePriority(header, param);
  }

  /**
   * See if the method has an annotation.
   * @return Return the priority from the annotation. If there isn't an annotation, this returns
   *         something below zero.
   */
  protected int getAnnotatedPriority(RequestHeader header) {
    String methodName = header.getMethodName();
    Integer priorityByAnnotation = annotatedQos.get(methodName);
    if (priorityByAnnotation != null) {
      return normalizePriority(priorityByAnnotation);
    }
    return -1;
  }

  protected abstract int normalizePriority(int priority);

  /**
   * Get the priority for a given request from the header and the param.
   * <p/>
   * This doesn't consider which user is sending the request at all.
   * <p/>
   * This doesn't consider annotations
   */
  protected abstract int getBasePriority(RequestHeader header, Message param);
}
