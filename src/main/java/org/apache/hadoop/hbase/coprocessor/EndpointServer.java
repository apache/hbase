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
package org.apache.hadoop.hbase.coprocessor;

import java.lang.reflect.Method;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

/**
 * A endpoint server.
 */
public class EndpointServer {

  private HRegionServer server;

  public EndpointServer(HRegionServer server) {
    this.server = server;
  }

  /**
   * Calls an endpoint on an region server.
   *
   * TODO make regionName a list.
   *
   * @param epName
   *          the endpoint name.
   * @param methodName
   *          the method name.
   * @param regionName
   *          the name of the region
   * @param startRow
   *          the start row, inclusive
   * @param stopRow
   *          the stop row, exclusive
   * @return the computed value.
   */
  public byte[] callEndpoint(String epName, String methodName,
      final byte[] regionName, final byte[] startRow, final byte[] stopRow)
      throws ThriftHBaseException {
    try {
      IEndpointFactory<?> fact = EndpointManager.get().getFactory(epName);
      if (fact == null) {
        // TODO daviddeng make a special exception for this
        throw new DoNotRetryIOException("Endpoint " + epName
            + " does not exists");
      }
      IEndpoint ep = fact.create();

      ep.setContext(new IEndpointContext() {
        @Override
        public HRegion getRegion() throws NotServingRegionException {
          return EndpointServer.this.server.getRegion(regionName);
        }

        @Override
        public byte[] getStartRow() {
          return startRow;
        }

        @Override
        public byte[] getStopRow() {
          return stopRow;
        }
      });

      // TODO daviddeng: now we only support methods without any parameters.
      Method mth = ep.getClass().getMethod(methodName);
      return (byte[]) mth.invoke(ep);
    } catch (Exception e) {
      // TODO daviddeng if the method is not found, should throw
      // DoNotRetryIOException
      throw new ThriftHBaseException(e);
    }
  }
}
