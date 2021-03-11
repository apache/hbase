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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Hbck fixup tool APIs. Obtain an instance from {@link ClusterConnection#getHbck()} and call
 * {@link #close()} when done.
 * <p>WARNING: the below methods can damage the cluster. It may leave the cluster in an
 * indeterminate state, e.g. region not assigned, or some hdfs files left behind. After running
 * any of the below, operators may have to do some clean up on hdfs or schedule some assign
 * procedures to get regions back online. DO AT YOUR OWN RISK. For experienced users only.
 *
 * @see ConnectionFactory
 * @see ClusterConnection
 * @since 2.0.2, 2.1.1
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.HBCK)
public interface Hbck extends Abortable, Closeable {
  /**
   * Update table state in Meta only. No procedures are submitted to open/assign or
   * close/unassign regions of the table.
   * @param state table state
   * @return previous state of the table in Meta
   */
  TableState setTableStateInMeta(TableState state) throws IOException;

  /**
   * Update region state in Meta only. No procedures are submitted to manipulate the given region or
   * any other region from same table.
   * @param nameOrEncodedName2State list of all region states to be updated in meta
   * @return previous state of the region in Meta
   */
  Map<String, RegionState.State>
    setRegionStateInMeta(Map<String, RegionState.State> nameOrEncodedName2State) throws IOException;

  /**
   * Like {@link Admin#assign(byte[])} but 'raw' in that it can do more than one Region at a time
   * -- good if many Regions to online -- and it will schedule the assigns even in the case where
   * Master is initializing (as long as the ProcedureExecutor is up). Does NOT call Coprocessor
   * hooks.
   * @param override You need to add the override for case where a region has previously been
   *              bypassed. When a Procedure has been bypassed, a Procedure will have completed
   *              but no other Procedure will be able to make progress on the target entity
   *              (intentionally). This override flag will override this fencing mechanism.
   * @param encodedRegionNames Region encoded names; e.g. 1588230740 is the hard-coded encoding
   *                           for hbase:meta region and de00010733901a05f5a2a3a382e27dd4 is an
   *                           example of what a random user-space encoded Region name looks like.
   */
  List<Long> assigns(List<String> encodedRegionNames, boolean override) throws IOException;

  default List<Long> assigns(List<String> encodedRegionNames) throws IOException {
    return assigns(encodedRegionNames, false);
  }

  /**
   * Like {@link Admin#unassign(byte[], boolean)} but 'raw' in that it can do more than one Region
   * at a time -- good if many Regions to offline -- and it will schedule the assigns even in the
   * case where Master is initializing (as long as the ProcedureExecutor is up). Does NOT call
   * Coprocessor hooks.
   * @param override You need to add the override for case where a region has previously been
   *              bypassed. When a Procedure has been bypassed, a Procedure will have completed
   *              but no other Procedure will be able to make progress on the target entity
   *              (intentionally). This override flag will override this fencing mechanism.
   * @param encodedRegionNames Region encoded names; e.g. 1588230740 is the hard-coded encoding
   *                           for hbase:meta region and de00010733901a05f5a2a3a382e27dd4 is an
   *                           example of what a random user-space encoded Region name looks like.
   */
  List<Long> unassigns(List<String> encodedRegionNames, boolean override) throws IOException;

  default List<Long> unassigns(List<String> encodedRegionNames) throws IOException {
    return unassigns(encodedRegionNames, false);
  }

  /**
   * Bypass specified procedure and move it to completion. Procedure is marked completed but
   * no actual work is done from the current state/step onwards. Parents of the procedure are
   * also marked for bypass.
   *
   * @param pids of procedures to complete.
   * @param waitTime wait time in ms for acquiring lock for a procedure
   * @param override if override set to true, we will bypass the procedure even if it is executing.
   *   This is for procedures which can't break out during execution (bugs?).
   * @param recursive If set, if a parent procedure, we will find and bypass children and then
   *   the parent procedure (Dangerous but useful in case where child procedure has been 'lost').
   *   Does not always work. Experimental.
   * @return true if procedure is marked for bypass successfully, false otherwise
   */
  List<Boolean> bypassProcedure(List<Long> pids, long waitTime, boolean override, boolean recursive)
      throws IOException;

  /**
   * Use {@link #scheduleServerCrashProcedures(List)} instead.
   * @deprecated since 2.2.1. Will removed in 3.0.0.
   */
  @Deprecated
  default List<Long> scheduleServerCrashProcedure(List<HBaseProtos.ServerName> serverNames)
      throws IOException {
    return scheduleServerCrashProcedures(
        serverNames.stream().map(ProtobufUtil::toServerName).collect(Collectors.toList()));
  }

  List<Long> scheduleServerCrashProcedures(List<ServerName> serverNames) throws IOException;

  List<Long> scheduleSCPsForUnknownServers() throws IOException;

  /**
   * Request HBCK chore to run at master side.
   *
   * @return <code>true</code> if HBCK chore ran, <code>false</code> if HBCK chore already running
   * @throws IOException if a remote or network exception occurs
   */
  boolean runHbckChore() throws IOException;

  /**
   * Fix Meta.
   */
  void fixMeta() throws IOException;
}
