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
package org.apache.hadoop.hbase.replication.regionserver;

import java.util.OptionalLong;

import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used by replication to prevent replicating unacked log entries. See
 * https://issues.apache.org/jira/browse/HBASE-14004 for more details.
 * WALFileLengthProvider exists because we do not want to reference WALFactory and WALProvider
 * directly in the replication code so in the future it will be easier to decouple them.
 * Each walProvider will have its own implementation.
 */
@InterfaceAudience.Private
@FunctionalInterface
public interface WALFileLengthProvider {

  OptionalLong getLogFileSizeIfBeingWritten(Path path);
}
