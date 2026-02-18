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
 * Factory for creating request attributes. Called each time a client call is started, allowing
 * dynamic attributes per call. Useful for propagating {@link ThreadLocal} context as request
 * attributes.
 * <p>
 * For a fixed set of attributes that does not change, use {@link FixedRequestAttributesFactory}.
 * @see AsyncTableBuilder#setRequestAttributesFactory(RequestAttributesFactory)
 */
@InterfaceAudience.Public
public interface RequestAttributesFactory {

  /**
   * Creates request attributes for a client call (e.g., {@link AsyncTable#get},
   * {@link AsyncTable#put}, {@link AsyncTable#scan}).
   * <p>
   * Guaranteed to be called on the same thread that initiates the client call.
   * @return the request attributes, must not be null
   */
  Map<String, byte[]> create();
}
