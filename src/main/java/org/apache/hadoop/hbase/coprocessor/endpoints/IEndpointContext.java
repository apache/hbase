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

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * The context of an endpoint calling.
 *
 * TODO add more functions if necessary for access more resource on the server.
 */
public interface IEndpointContext {
  /**
   * Returns an HRegion instance.
   *
   * @throws NotServingRegionException
   *           if the region is not served on this server.
   */
  HRegion getRegion() throws NotServingRegionException;

  /**
   * The start row, inclusive, within this region of this call.
   */
  byte[] getStartRow();

  /**
   * The stop row, exclusive, within this region of this call.
   */
  byte[] getStopRow();
}
