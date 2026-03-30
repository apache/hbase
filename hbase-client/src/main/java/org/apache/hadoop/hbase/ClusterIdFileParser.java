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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Generic parser interface for Cluster Id files.
 * @see ClusterIdFile
 */
@InterfaceAudience.Private
public interface ClusterIdFileParser<T> {

  /**
   * Get default file name of cluster id file.
   */
  String getFileName();

  /**
   * Parse cluster id data from byte representation.
   * @param bytes the protobuf data
   * @return the cluster id data object
   */
  T parseFrom(final byte[] bytes) throws DeserializationException;

  /**
   * Parser cluster id data from String representation.
   * @param input the input string
   * @return the cluster id data object
   */
  T readString(String input);
}
