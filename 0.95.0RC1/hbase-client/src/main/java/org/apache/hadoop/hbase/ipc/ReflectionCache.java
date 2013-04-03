/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.IpcProtocol;


import com.google.protobuf.Message;

/**
 * Save on relection by keeping around method, method argument, and constructor instances
 */
class ReflectionCache {
  private final Map<String, Message> methodArgCache = new ConcurrentHashMap<String, Message>();
  private final Map<String, Method> methodInstanceCache = new ConcurrentHashMap<String, Method>();

  public ReflectionCache() {
    super();
  }

  Method getMethod(Class<? extends IpcProtocol> protocol, String methodName) {
    Method method = this.methodInstanceCache.get(methodName);
    if (method != null) return method;
    Method [] methods = protocol.getMethods();
    for (Method m : methods) {
      if (m.getName().equals(methodName)) {
        m.setAccessible(true);
        this.methodInstanceCache.put(methodName, m);
        return m;
      }
    }
    return null;
  }

  Message getMethodArgType(Method method) throws Exception {
    Message protoType = this.methodArgCache.get(method.getName());
    if (protoType != null) return protoType;
    Class<?>[] args = method.getParameterTypes();
    Class<?> arg;
    if (args.length == 2) {
      // RpcController + Message in the method args
      // (generated code from RPC bits in .proto files have RpcController)
      arg = args[1];
    } else if (args.length == 1) {
      arg = args[0];
    } else {
      //unexpected
      return null;
    }
    //in the protobuf methods, args[1] is the only significant argument
    Method newInstMethod = arg.getMethod("getDefaultInstance");
    newInstMethod.setAccessible(true);
    protoType = (Message) newInstMethod.invoke(null, (Object[]) null);
    this.methodArgCache.put(method.getName(), protoType);
    return protoType;
  }
}