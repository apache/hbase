/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.ipc.thrift;

import com.facebook.swift.service.ThriftClientManager;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

public interface ThriftClientCache {

  /**
   * Gives a Thrift client given the InetSocketAddress and class name for
   * the ThriftClientInterface
   *
   * @param address
   * @param clazz
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws Exception
   */
  public ThriftClientInterface getClient(
      InetSocketAddress address, Class<? extends ThriftClientInterface> clazz)
      throws InterruptedException, ExecutionException, Exception;

  /**
   * Put back the ThriftClientInterface that was taken using the
   * {@link ThriftClientCache#getClient(InetSocketAddress, Class)}
   * @param server
   * @param addr
   * @param clazz
   * @throws Exception
   */
  public void putBackClient(ThriftClientInterface server, InetSocketAddress addr,
      Class<? extends ThriftClientInterface> clazz) throws Exception;

  /**
   * Getter for the ThriftClientManager
   * @return
   */
  public ThriftClientManager getThriftClientManager();

  /**
   * Closes the connection
   * @param address
   * @param clazz
   * @param client
   * @throws IOException
   */
  void close(InetSocketAddress address,
      Class<? extends ThriftClientInterface> clazz, ThriftClientInterface client)
      throws IOException;

  /**
   * Shuts down the client manager and also the connections present in the pools
   * @throws Exception
   */
  public void shutDownClientManager() throws Exception;
}
