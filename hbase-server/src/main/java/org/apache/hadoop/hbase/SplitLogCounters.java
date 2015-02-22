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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Counters kept by the distributed WAL split log process.
 * Used by master and regionserver packages.
 */
@InterfaceAudience.Private
public class SplitLogCounters {
  //SplitLogManager counters
  public final static AtomicLong tot_mgr_log_split_batch_start = new AtomicLong(0);
  public final static AtomicLong tot_mgr_log_split_batch_success = new AtomicLong(0);
  public final static AtomicLong tot_mgr_log_split_batch_err = new AtomicLong(0);
  public final static AtomicLong tot_mgr_new_unexpected_wals = new AtomicLong(0);
  public final static AtomicLong tot_mgr_log_split_start = new AtomicLong(0);
  public final static AtomicLong tot_mgr_log_split_success = new AtomicLong(0);
  public final static AtomicLong tot_mgr_log_split_err = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_create_queued = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_create_result = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_already_exists = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_create_err = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_create_retry = new AtomicLong(0);
  public final static AtomicLong tot_mgr_get_data_queued = new AtomicLong(0);
  public final static AtomicLong tot_mgr_get_data_result = new AtomicLong(0);
  public final static AtomicLong tot_mgr_get_data_nonode = new AtomicLong(0);
  public final static AtomicLong tot_mgr_get_data_err = new AtomicLong(0);
  public final static AtomicLong tot_mgr_get_data_retry = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_delete_queued = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_delete_result = new AtomicLong(0);
  public final static AtomicLong tot_mgr_node_delete_err = new AtomicLong(0);
  public final static AtomicLong tot_mgr_resubmit = new AtomicLong(0);
  public final static AtomicLong tot_mgr_resubmit_failed = new AtomicLong(0);
  public final static AtomicLong tot_mgr_null_data = new AtomicLong(0);
  public final static AtomicLong tot_mgr_orphan_task_acquired = new AtomicLong(0);
  public final static AtomicLong tot_mgr_wait_for_zk_delete = new AtomicLong(0);
  public final static AtomicLong tot_mgr_unacquired_orphan_done = new AtomicLong(0);
  public final static AtomicLong tot_mgr_resubmit_threshold_reached = new AtomicLong(0);
  public final static AtomicLong tot_mgr_missing_state_in_delete = new AtomicLong(0);
  public final static AtomicLong tot_mgr_heartbeat = new AtomicLong(0);
  public final static AtomicLong tot_mgr_rescan = new AtomicLong(0);
  public final static AtomicLong tot_mgr_rescan_deleted = new AtomicLong(0);
  public final static AtomicLong tot_mgr_task_deleted = new AtomicLong(0);
  public final static AtomicLong tot_mgr_resubmit_unassigned = new AtomicLong(0);
  public final static AtomicLong tot_mgr_relist_logdir = new AtomicLong(0);
  public final static AtomicLong tot_mgr_resubmit_dead_server_task = new AtomicLong(0);
  public final static AtomicLong tot_mgr_resubmit_force = new AtomicLong(0);

  // SplitLogWorker counters
  public final static AtomicLong tot_wkr_failed_to_grab_task_no_data = new AtomicLong(0);
  public final static AtomicLong tot_wkr_failed_to_grab_task_exception = new AtomicLong(0);
  public final static AtomicLong tot_wkr_failed_to_grab_task_owned = new AtomicLong(0);
  public final static AtomicLong tot_wkr_failed_to_grab_task_lost_race = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_acquired = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_resigned = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_done = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_err = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_heartbeat = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_acquired_rescan = new AtomicLong(0);
  public final static AtomicLong tot_wkr_get_data_queued = new AtomicLong(0);
  public final static AtomicLong tot_wkr_get_data_result = new AtomicLong(0);
  public final static AtomicLong tot_wkr_get_data_retry = new AtomicLong(0);
  public final static AtomicLong tot_wkr_preempt_task = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_heartbeat_failed = new AtomicLong(0);
  public final static AtomicLong tot_wkr_final_transition_failed = new AtomicLong(0);
  public final static AtomicLong tot_wkr_task_grabing = new AtomicLong(0);

  public static void resetCounters() throws Exception {
    Class<?> cl = SplitLogCounters.class;
    for (Field fld : cl.getDeclaredFields()) {
      if (!fld.isSynthetic()) ((AtomicLong)fld.get(null)).set(0);
    }
  }
}
