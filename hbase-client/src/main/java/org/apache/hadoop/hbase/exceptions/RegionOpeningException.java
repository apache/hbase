/*
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
package org.apache.hadoop.hbase.exceptions;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass if the server knows the region is now on another server.
 * This allows the client to call the new region server without calling the master.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionOpeningException extends NotServingRegionException {
  private static final Logger LOG = LoggerFactory.getLogger(RegionOpeningException.class);
  private static final long serialVersionUID = -7232903522310558395L;

  public RegionOpeningException(String message) {
    super(message);
  }
}
