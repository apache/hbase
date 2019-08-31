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

import java.util.ArrayList;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used by {@link HBaseFsck} reporting system.
 * @deprecated Since 2.3.0. To be removed in hbase4. Use HBCK2 instead. Remove when
 *   {@link HBaseFsck} is removed.
 */
@Deprecated
@InterfaceAudience.Private
public interface HbckErrorReporter {
  enum ERROR_CODE {
    UNKNOWN, NO_META_REGION, NULL_META_REGION, NO_VERSION_FILE, NOT_IN_META_HDFS, NOT_IN_META,
    NOT_IN_META_OR_DEPLOYED, NOT_IN_HDFS_OR_DEPLOYED, NOT_IN_HDFS, SERVER_DOES_NOT_MATCH_META,
    NOT_DEPLOYED, MULTI_DEPLOYED, SHOULD_NOT_BE_DEPLOYED, MULTI_META_REGION, RS_CONNECT_FAILURE,
    FIRST_REGION_STARTKEY_NOT_EMPTY, LAST_REGION_ENDKEY_NOT_EMPTY, DUPE_STARTKEYS,
    HOLE_IN_REGION_CHAIN, OVERLAP_IN_REGION_CHAIN, REGION_CYCLE, DEGENERATE_REGION,
    ORPHAN_HDFS_REGION, LINGERING_SPLIT_PARENT, NO_TABLEINFO_FILE, LINGERING_REFERENCE_HFILE,
    LINGERING_HFILELINK, WRONG_USAGE, EMPTY_META_CELL, EXPIRED_TABLE_LOCK, BOUNDARIES_ERROR,
    ORPHAN_TABLE_STATE, NO_TABLE_STATE, UNDELETED_REPLICATION_QUEUE, DUPE_ENDKEYS,
    UNSUPPORTED_OPTION, INVALID_TABLE
  }

  void clear();

  void report(String message);

  void reportError(String message);

  void reportError(ERROR_CODE errorCode, String message);

  void reportError(ERROR_CODE errorCode, String message, HbckTableInfo table);

  void reportError(ERROR_CODE errorCode, String message, HbckTableInfo table, HbckRegionInfo info);

  void reportError(ERROR_CODE errorCode, String message, HbckTableInfo table, HbckRegionInfo info1,
      HbckRegionInfo info2);

  int summarize();

  void detail(String details);

  ArrayList<ERROR_CODE> getErrorList();

  void progress();

  void print(String message);

  void resetErrors();

  boolean tableHasErrors(HbckTableInfo table);
}
