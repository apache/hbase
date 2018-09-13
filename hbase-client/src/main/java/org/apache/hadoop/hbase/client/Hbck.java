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

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hbck APIs for HBase. Obtain an instance from {@link ClusterConnection#getHbck()} and call
 * {@link #close()} when done.
 * <p>Hbck client APIs will be mostly used by hbck tool which in turn can be used by operators to
 * fix HBase and bringing it to consistent state.</p>
 *
 * <p>NOTE: The methods in here can do damage to a cluster if applied in the wrong sequence or at
 * the wrong time. Use with caution. For experts only. These methods are only for the
 * extreme case where the cluster has been damaged or has achieved an inconsistent state because
 * of some unforeseen circumstance or bug and requires manual intervention.
 *
 * @see ConnectionFactory
 * @see ClusterConnection
 * @since 2.2.0
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.HBCK)
public interface Hbck extends Abortable, Closeable {
  /**
   * Update table state in Meta only. No procedures are submitted to open/assign or
   * close/unassign regions of the table.
   *
   * <p>>NOTE: This is a dangerous action, as existing running procedures for the table or regions
   * which belong to the table may get confused.
   *
   * @param state table state
   * @return previous state of the table in Meta
   */
  TableState setTableStateInMeta(TableState state) throws IOException;

  /**
   * Like {@link Admin#assign(byte[])} but 'raw' in that it can do more than one Region at a time
   * -- good if many Regions to online -- and it will schedule the assigns even in the case where
   * Master is initializing (as long as the ProcedureExecutor is up). Does NOT call Coprocessor
   * hooks.
   * @param encodedRegionNames Region encoded names; e.g. 1588230740 is the hard-coded encoding
   *                           for hbase:meta region and de00010733901a05f5a2a3a382e27dd4 is an
   *                           example of what a random user-space encoded Region name looks like.
   */
  List<Long> assigns(List<String> encodedRegionNames) throws IOException;

  /**
   * Like {@link Admin#unassign(byte[], boolean)} but 'raw' in that it can do more than one Region
   * at a time -- good if many Regions to offline -- and it will schedule the assigns even in the
   * case where Master is initializing (as long as the ProcedureExecutor is up). Does NOT call
   * Coprocessor hooks.
   * @param encodedRegionNames Region encoded names; e.g. 1588230740 is the hard-coded encoding
   *                           for hbase:meta region and de00010733901a05f5a2a3a382e27dd4 is an
   *                           example of what a random user-space encoded Region name looks like.
   */
  List<Long> unassigns(List<String> encodedRegionNames) throws IOException;
}
