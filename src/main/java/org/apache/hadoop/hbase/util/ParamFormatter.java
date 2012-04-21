/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;
import java.util.Map;

/**
 * Interface for pretty printing information about a method call.
 * Each implementing class is (usually) specific for one method,
 * and is assigned with the ParamFormat annotation.
 *
 * @see org.apache.hadoop.hbase.regionserver.HRegionServer#ScanParamsFormatter
 */
public interface ParamFormatter<TContext> {
  /**
   * Returns information about a method call given params and context.
   *
   * @param params The params that are/were passed to the method
   * @param context Usually the instance the method is called on.
   * @return A map of information about the method call
   */
  Map<String, Object> getMap(Object[] params, TContext context);
}
