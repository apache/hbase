/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.idx.support;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Holds class sizes used by Idx heap size calcs.
 * TODO merge with ClassSize.
 */
public class IdxClassSize extends ClassSize {

  /**
   * Hash map fixed overhead.
   */
  public static final long HASHMAP = align(OBJECT + 3 * Bytes.SIZEOF_INT +
    Bytes.SIZEOF_FLOAT + ARRAY + 4 * REFERENCE);

  /**
   * Object array list fixed overhead.
   */
  public static final long OBJECT_ARRAY_LIST = align(OBJECT + Bytes.SIZEOF_INT +
  ARRAY + REFERENCE);


}
