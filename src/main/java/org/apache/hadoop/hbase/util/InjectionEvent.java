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
package org.apache.hadoop.hbase.util;

/**
 * Enumeration of all injection events.
 * When defining new events, please PREFIX the name
 * with the supervised class.
 *
 * Please see InjectionHandler.
 */
public enum InjectionEvent {
  HMASTER_CREATE_TABLE,
  HMASTER_DELETE_TABLE,
  HMASTER_ALTER_TABLE,
  HMASTER_ENABLE_TABLE,
  HMASTER_DISABLE_TABLE,
  ZKUNASSIGNEDWATCHER_REGION_OPENED,
  SPLITLOGWORKER_SPLIT_LOG_START,
  HMASTER_START_PROCESS_DEAD_SERVER,
  HREGIONSERVER_REPORT_RESPONSE,

  // Injection into Store.java
  READONLYSTORE_COMPACTION_WHILE_SNAPSHOTTING,
  STORESCANNER_COMPACTION_RACE,
  STOREFILE_AFTER_WRITE_CLOSE,
  STOREFILE_AFTER_RENAME
}
