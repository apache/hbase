/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.endpoints.EndpointBytesCodec.IBytesDecoder;

/**
 * The manager holding all endpoint factories in the server.
 *
 */
public class EndpointManager implements IEndpointRegistry {
  /**
   * Data-structure storing information of an Endpoint.
   */
  public static class EndpointInfo {
    private IEndpointFactory<?> factory;
    private Map<String, Method> methods = new HashMap<>();
    private Map<String, IBytesDecoder[]> mthToDecs = new ConcurrentHashMap<>();

    private Object[] encodeParams(String methodKey, List<byte[]> params) {
      IBytesDecoder[] decoders = mthToDecs.get(methodKey);
      Object[] res = new Object[params.size()];
      for (int i = 0; i < res.length; i++) {
        res[i] = decoders[i].decode(params.get(i));
      }
      return res;
    }

    private static String makeMethodKey(String methodName, int nParams) {
      return methodName + "/" + nParams;
    }

    private static final HashSet<String> IEndpointMethodNames = new HashSet<>();
    static {
      for (Method method : IEndpoint.class.getMethods()) {
        IEndpointMethodNames.add(method.getName());
      }
    }

    /**
     * Constructor.
     *
     * @param iEndpoint the class of the IEndpont instance.
     * @param factory the factory generating IEndpoint instances.
     */
    public EndpointInfo(Class<?> iEndpoint, IEndpointFactory<?> factory) {
      this.factory = factory;

      for (Method method : iEndpoint.getMethods()) {
        if (IEndpointMethodNames.contains(method.getName())) {
          // Ignore methods in IEndpoint
          continue;
        }

        Class<?>[] paramsCls = method.getParameterTypes();

        String key = makeMethodKey(method.getName(), paramsCls.length);
        this.methods.put(key, method);

        EndpointBytesCodec.IBytesDecoder[] decs =
            new EndpointBytesCodec.IBytesDecoder[paramsCls.length];
        for (int i = 0; i < paramsCls.length; i++) {
          IBytesDecoder dec = EndpointBytesCodec.findDecoder(paramsCls[i]);
          if (dec == null) {
            throw new UnsupportedTypeException(paramsCls[i]);
          }
          decs[i] = dec;
        }
        this.mthToDecs.put(key, decs);
      }
    }

    /**
     * Calls factory to create a new instance of IEndpoint.
     */
    public IEndpoint createEndpoint() {
      return factory.create();
    }

    /**
     * Invokes a method in the Endpoint.
     *
     * @param ep the IEndpoint instance.
     * @param methodName the name of the methods.
     * @param params the encoded parameters.
     * @return the encoded return results.
     */
    public byte[] invoke(IEndpoint ep, String methodName, List<byte[]> params)
        throws IllegalAccessException, IllegalArgumentException,
        InvocationTargetException, IOException {
      String methodKey = EndpointInfo.makeMethodKey(methodName, params.size());
      Method mth = methods.get(methodKey);
      if (mth == null) {
        // TODO daviddeng make a special exception for this
        throw new DoNotRetryIOException("epName." + methodKey
            + " does not exists");
      }
      return EndpointBytesCodec.encodeObject(mth.invoke(ep,
          encodeParams(methodKey, params)));
    }
  }

  private ConcurrentHashMap<String, EndpointInfo> nameFacts =
      new ConcurrentHashMap<>();

  /**
   * Returns the factory of an endpoint.
   */
  public EndpointInfo getEndpointEntry(String name) {
    return nameFacts.get(name);
  }

  @Override
  public <T extends IEndpoint> void register(Class<T> iEndpoint,
      IEndpointFactory<T> factory) {
    nameFacts.put(iEndpoint.getName(), new EndpointInfo(iEndpoint, factory));
  }

  @Override
  public void unregisterAll() {
    nameFacts.clear();
  }
}
