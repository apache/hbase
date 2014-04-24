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

import java.util.List;

import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

/**
 * The interface of a server executing endpoints.
 *
 * Methods defined here are redefined in ThriftHRegionInterface and annotated.
 * The reason is Thrift doesn't support multi inheritance even for interface.
 */
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
  public byte[] callEndpoint(String epName, String methodName,
      List<byte[]> params, byte[] regionName, byte[] startRow,
      byte[] stopRow) throws ThriftHBaseException;
}
