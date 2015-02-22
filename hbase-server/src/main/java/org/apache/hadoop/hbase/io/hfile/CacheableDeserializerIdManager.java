/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This class is used to manage the identifiers for
 * {@link CacheableDeserializer}
 */
@InterfaceAudience.Private
public class CacheableDeserializerIdManager {
  private static final Map<Integer, CacheableDeserializer<Cacheable>> registeredDeserializers =
    new HashMap<Integer, CacheableDeserializer<Cacheable>>();
  private static final AtomicInteger identifier = new AtomicInteger(0);

  /**
   * Register the given cacheable deserializer and generate an unique identifier
   * id for it
   * @param cd
   * @return the identifier of given cacheable deserializer
   */
  public static int registerDeserializer(CacheableDeserializer<Cacheable> cd) {
    int idx = identifier.incrementAndGet();
    synchronized (registeredDeserializers) {
      registeredDeserializers.put(idx, cd);
    }
    return idx;
  }

  /**
   * Get the cacheable deserializer as the given identifier Id
   * @param id
   * @return CacheableDeserializer
   */
  public static CacheableDeserializer<Cacheable> getDeserializer(int id) {
    return registeredDeserializers.get(id);
  }
}
