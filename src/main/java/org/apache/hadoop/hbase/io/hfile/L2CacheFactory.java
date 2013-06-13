/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;

/**
 * Abstract factory for {@link L2Cache}
 */
public interface L2CacheFactory {

  /**
   * Given a configuration, create, configure, and return an L2 cache
   * instance. The implementation may cache an already created L2 cache
   * instance and return the cached instance instead.
   * @param conf The HBase configuration needed to create the instance
   * @return The created and configured instance
   */
  public L2Cache getL2Cache(Configuration conf);
}
