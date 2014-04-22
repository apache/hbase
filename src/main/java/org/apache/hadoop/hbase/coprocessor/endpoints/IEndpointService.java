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

import java.util.ArrayList;

import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

/**
 * The interface of a server executing endpoints.
 */
@ThriftService
public interface IEndpointService {
  /**
   * Calls an endpoint on an region server.
   *
   * TODO make regionName/startRow/stopRow a list.
   *
   * @param epName the endpoint name.
   * @param methodName the method name.
   * @param regionName the name of the region
   * @param startRow the start row, inclusive
   * @param stopRow the stop row, exclusive
   * @return the computed value.
   */
  @ThriftMethod(value = "callEndpoint", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public byte[] callEndpoint(@ThriftField(name = "epName") String epName,
      @ThriftField(name = "methodName") String methodName,
      @ThriftField(name = "params") ArrayList<byte[]> params,
      @ThriftField(name = "regionName") byte[] regionName,
      @ThriftField(name = "startRow") byte[] startRow,
      @ThriftField(name = "stopRow") byte[] stopRow)
      throws ThriftHBaseException;
}
