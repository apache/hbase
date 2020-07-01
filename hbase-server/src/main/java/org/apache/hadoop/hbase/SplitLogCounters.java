package org.apache.hadoop.hbase;

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
import java.lang.reflect.Field;
import java.util.concurrent.atomic.LongAdder;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Counters kept by the distributed WAL split log process.
 * Used by master and regionserver packages.
 * @deprecated since 2.4.0 and in 3.0.0, to be removed in 4.0.0, replaced by procedure-based
 *   distributed WAL splitter, see SplitWALManager
 */
@Deprecated
@InterfaceAudience.Private
public class SplitLogCounters {
  private SplitLogCounters() {}

  //Spnager counters
  public final static LongAdder tot_mgr_log_split_batch_start = new LongAdder();
  public final static LongAdder tot_mgr_log_split_batch_success = new LongAdder();
  public final static LongAdder tot_mgr_log_split_batch_err = new LongAdder();
  public final static LongAdder tot_mgr_new_unexpected_wals = new LongAdder();
  public final static LongAdder tot_mgr_log_split_start = new LongAdder();
  public final static LongAdder tot_mgr_log_split_success = new LongAdder();
  public final static LongAdder tot_mgr_log_split_err = new LongAdder();
  public final static LongAdder tot_mgr_node_create_queued = new LongAdder();
  public final static LongAdder tot_mgr_node_create_result = new LongAdder();
  public final static LongAdder tot_mgr_node_already_exists = new LongAdder();
  public final static LongAdder tot_mgr_node_create_err = new LongAdder();
  public final static LongAdder tot_mgr_node_create_retry = new LongAdder();
  public final static LongAdder tot_mgr_get_data_queued = new LongAdder();
  public final static LongAdder tot_mgr_get_data_result = new LongAdder();
  public final static LongAdder tot_mgr_get_data_nonode = new LongAdder();
  public final static LongAdder tot_mgr_get_data_err = new LongAdder();
  public final static LongAdder tot_mgr_get_data_retry = new LongAdder();
  public final static LongAdder tot_mgr_node_delete_queued = new LongAdder();
  public final static LongAdder tot_mgr_node_delete_result = new LongAdder();
  public final static LongAdder tot_mgr_node_delete_err = new LongAdder();
  public final static LongAdder tot_mgr_resubmit = new LongAdder();
  public final static LongAdder tot_mgr_resubmit_failed = new LongAdder();
  public final static LongAdder tot_mgr_null_data = new LongAdder();
  public final static LongAdder tot_mgr_orphan_task_acquired = new LongAdder();
  public final static LongAdder tot_mgr_wait_for_zk_delete = new LongAdder();
  public final static LongAdder tot_mgr_unacquired_orphan_done = new LongAdder();
  public final static LongAdder tot_mgr_resubmit_threshold_reached = new LongAdder();
  public final static LongAdder tot_mgr_missing_state_in_delete = new LongAdder();
  public final static LongAdder tot_mgr_heartbeat = new LongAdder();
  public final static LongAdder tot_mgr_rescan = new LongAdder();
  public final static LongAdder tot_mgr_rescan_deleted = new LongAdder();
  public final static LongAdder tot_mgr_task_deleted = new LongAdder();
  public final static LongAdder tot_mgr_resubmit_unassigned = new LongAdder();
  public final static LongAdder tot_mgr_relist_logdir = new LongAdder();
  public final static LongAdder tot_mgr_resubmit_dead_server_task = new LongAdder();
  public final static LongAdder tot_mgr_resubmit_force = new LongAdder();

  // SplitLogWorker counters
  public final static LongAdder tot_wkr_failed_to_grab_task_no_data = new LongAdder();
  public final static LongAdder tot_wkr_failed_to_grab_task_exception = new LongAdder();
  public final static LongAdder tot_wkr_failed_to_grab_task_owned = new LongAdder();
  public final static LongAdder tot_wkr_failed_to_grab_task_lost_race = new LongAdder();
  public final static LongAdder tot_wkr_task_acquired = new LongAdder();
  public final static LongAdder tot_wkr_task_resigned = new LongAdder();
  public final static LongAdder tot_wkr_task_done = new LongAdder();
  public final static LongAdder tot_wkr_task_err = new LongAdder();
  public final static LongAdder tot_wkr_task_heartbeat = new LongAdder();
  public final static LongAdder tot_wkr_task_acquired_rescan = new LongAdder();
  public final static LongAdder tot_wkr_get_data_queued = new LongAdder();
  public final static LongAdder tot_wkr_get_data_result = new LongAdder();
  public final static LongAdder tot_wkr_get_data_retry = new LongAdder();
  public final static LongAdder tot_wkr_preempt_task = new LongAdder();
  public final static LongAdder tot_wkr_task_heartbeat_failed = new LongAdder();
  public final static LongAdder tot_wkr_final_transition_failed = new LongAdder();
  public final static LongAdder tot_wkr_task_grabing = new LongAdder();

  public static void resetCounters() throws Exception {
    Class<?> cl = SplitLogCounters.class;
    for (Field fld : cl.getDeclaredFields()) {
      /* Guard against source instrumentation. */
      if ((!fld.isSynthetic()) && (LongAdder.class.isAssignableFrom(fld.getType()))) {
        ((LongAdder)fld.get(null)).reset();
      }
    }
  }
}
