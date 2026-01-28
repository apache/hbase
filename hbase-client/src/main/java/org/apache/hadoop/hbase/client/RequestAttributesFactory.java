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
 */
package org.apache.hadoop.hbase.client;

import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A factory for creating request attributes. This is called each time a new call is started,
 * allowing for dynamic attributes based on the current context or existing attributes.
 * <p>
 * The {@link #create} method is guaranteed to be called on the same thread that initiates the
 * client call (e.g., {@link AsyncTable#get}, {@link AsyncTable#put}, {@link AsyncTable#scan},
 * etc.).
 */
@InterfaceAudience.Public
public interface RequestAttributesFactory {

  /**
   * A factory that returns the input attributes unchanged.
   */
  RequestAttributesFactory PASSTHROUGH = (requestAttributes) -> requestAttributes;

  /**
   * Creates a new map of request attributes based on the existing attributes for the table.
   * <p>
   * This method is guaranteed to be called on the same thread that initiates the client call.
   * @param requestAttributes The existing attributes configured on the table
   * @return The new map of request attributes. Must not be null.
   */
  Map<String, byte[]> create(Map<String, byte[]> requestAttributes);
}
