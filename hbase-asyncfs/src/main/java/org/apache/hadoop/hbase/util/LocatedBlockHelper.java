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
package org.apache.hadoop.hbase.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * hadoop 3.3.1 changed the return value of this method from {@code DatanodeInfo[]} to
 * {@code DatanodeInfoWithStorage[]}, which causes the JVM can not locate the method if we are
 * compiled with hadoop 3.2 and then link with hadoop 3.3+, so here we need to use reflection to
 * make it work for both hadoop versions, otherwise we need to publish more artifacts for different
 * hadoop versions...
 */
@InterfaceAudience.Private
public final class LocatedBlockHelper {

  private static final Method GET_LOCATED_BLOCK_LOCATIONS_METHOD;

  static {
    try {
      GET_LOCATED_BLOCK_LOCATIONS_METHOD = LocatedBlock.class.getMethod("getLocations");
    } catch (Exception e) {
      throw new Error("Can not initialize access to HDFS LocatedBlock.getLocations method", e);
    }
  }

  private LocatedBlockHelper() {
  }

  public static DatanodeInfo[] getLocatedBlockLocations(LocatedBlock block) {
    try {
      // DatanodeInfoWithStorage[] can be casted to DatanodeInfo[] directly
      return (DatanodeInfo[]) GET_LOCATED_BLOCK_LOCATIONS_METHOD.invoke(block);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
