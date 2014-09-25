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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Map from type T to int and vice-versa. Used for reducing bit field item
 * counts.
 */
@InterfaceAudience.Private
public final class UniqueIndexMap<T> implements Serializable {
  private static final long serialVersionUID = -1145635738654002342L;

  ConcurrentHashMap<T, Integer> mForwardMap = new ConcurrentHashMap<T, Integer>();
  ConcurrentHashMap<Integer, T> mReverseMap = new ConcurrentHashMap<Integer, T>();
  AtomicInteger mIndex = new AtomicInteger(0);

  // Map a length to an index. If we can't, allocate a new mapping. We might
  // race here and get two entries with the same deserialiser. This is fine.
  int map(T parameter) {
    Integer ret = mForwardMap.get(parameter);
    if (ret != null) return ret.intValue();
    int nexti = mIndex.incrementAndGet();
    assert (nexti < Short.MAX_VALUE);
    mForwardMap.put(parameter, nexti);
    mReverseMap.put(nexti, parameter);
    return nexti;
  }

  T unmap(int leni) {
    Integer len = Integer.valueOf(leni);
    assert mReverseMap.containsKey(len);
    return mReverseMap.get(len);
  }
}
