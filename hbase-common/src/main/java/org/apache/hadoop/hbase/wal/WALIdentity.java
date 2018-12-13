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
package org.apache.hadoop.hbase.wal;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This interface defines the identification of WAL for both stream based and distributed FileSystem
 * based environments.
 * See {@link #getName()} method.
 */
@InterfaceAudience.Private
public interface WALIdentity extends Comparable<WALIdentity> {

  /**
   * WALIdentity is uniquely identifying a WAL stored in this WALProvider.
   * This name can be thought of as a human-readable, serialized form of the WALIdentity.
   *
   * The same value should be returned across calls to this method.
   *
   * @return name of the wal
   */
  String getName();
}
