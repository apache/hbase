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
package org.apache.hadoop.hbase.replication;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Policy that defines what a replication endpoint should do when the entry batch is empty. This is
 * used to determine whether the replication source should consider an empty batch as: -
 * {@code COMMIT}: Consider the position as fully committed, and update the WAL position. -
 * {@code SUBMIT}: Treat it as submitted but not committed, i.e., do not advance the WAL position.
 * Some endpoints may buffer entries (e.g., in open files on S3) and delay actual persistence. In
 * such cases, an empty batch should not result in WAL position commit.
 */
@InterfaceAudience.Private
public enum EmptyEntriesPolicy {
  COMMIT,
  SUBMIT
}
