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
 *
 */

package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.ByteStringer;

/**
 * Utilities for handling coprocessor service calls.
 */
@InterfaceAudience.Private
public final class CoprocessorRpcUtils {
  /**
   * We assume that all HBase protobuf services share a common package name
   * (defined in the .proto files).
   */
  private static String hbaseServicePackage;
  static {
    Descriptors.ServiceDescriptor clientService = ClientProtos.ClientService.getDescriptor();
    hbaseServicePackage = clientService.getFullName()
        .substring(0, clientService.getFullName().lastIndexOf(clientService.getName()));
  }

  private CoprocessorRpcUtils() {
    // private for utility class
  }

  /**
   * Returns the name to use for coprocessor service calls.  For core HBase services
   * (in the hbase.pb protobuf package), this returns the unqualified name in order to provide
   * backward compatibility across the package name change.  For all other services,
   * the fully-qualified service name is used.
   */
  public static String getServiceName(Descriptors.ServiceDescriptor service) {
    if (service.getFullName().startsWith(hbaseServicePackage)) {
      return service.getName();
    }
    return service.getFullName();
  }

  /**
   * Returns a service call instance for the given coprocessor request.
   */
  public static ClientProtos.CoprocessorServiceCall buildServiceCall(byte[] row,
      Descriptors.MethodDescriptor method, Message request) {
    return ClientProtos.CoprocessorServiceCall.newBuilder()
        .setRow(ByteStringer.wrap(row))
        .setServiceName(CoprocessorRpcUtils.getServiceName(method.getService()))
        .setMethodName(method.getName())
        .setRequest(request.toByteString()).build();
  }
}
