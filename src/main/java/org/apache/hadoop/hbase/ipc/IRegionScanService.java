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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

/**
 * Interface of scanner service in a region.
 */
@ThriftService
public interface IRegionScanService {
  /**
   * Opens a scanner, optionally returns some data if numberOfRows > 0.
   *
   * @param regionName the name of the region to scan
   * @param scan the Scan instance defining scan query.
   * @param numberOfRows maximum number of rows to return after successfully
   *          open the scanner.
   * @return the result as a ScannerResult.
   *         The length of the Result list of the return value could be empty
   *         and EOR is set to true for sure in this case.
   */
  @ThriftMethod(value = "scanOpen", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  ScannerResult scanOpen(
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name = "scan") Scan scan,
      @ThriftField(name = "numberOfRows") int numberOfRows)
      throws ThriftHBaseException;

  /**
   * Returns next scanning results.
   *
   * @param ID the ID of the scanner
   * @param numberOfRows maximum number of rows to return,
   * @return the result as a ScannerResult.
   *         The length of the Result list of the return value could be empty
   *         and EOR is set to true for sure in this case.
   */
  @ThriftMethod(value = "scanNext", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  ScannerResult scanNext(
      @ThriftField(name="id") long id,
      @ThriftField(name = "numberOfRows") int numberOfRows)
      throws ThriftHBaseException;

  /**
   * Closes the scanner on the server side.
   *
   * @param id  the ID of the scanner to close
   * @return true  if a scanner is closed. false if the scanner doesn't exist.
   */
  @ThriftMethod(value = "scanClose", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  boolean scanClose(@ThriftField(name = "id") long id)
      throws ThriftHBaseException;
}
