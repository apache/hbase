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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import org.apache.hadoop.hbase.DistributedHBaseCluster;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseClusterManager;

/**
 * Base class for performing Actions based on linux commands requiring sudo privileges
 */
abstract public class SudoCommandAction extends Action {

  protected long timeout;
  protected HBaseClusterManager clusterManager;

  public SudoCommandAction(long timeout) {
    this.timeout = timeout;
  }

  @Override
  public void init(ActionContext context) throws IOException {
    super.init(context);
    HBaseCluster cluster = context.getHBaseCluster();
    if (cluster instanceof DistributedHBaseCluster){
      Object manager = ((DistributedHBaseCluster)cluster).getClusterManager();
      if (manager instanceof HBaseClusterManager){
        clusterManager = (HBaseClusterManager) manager;
      }
    }
  }

  @Override
  public void perform() throws Exception {
    if (clusterManager == null){
      getLogger().info("Couldn't perform command action, it requires a distributed cluster.");
      return;
    }

    // Don't try the modify if we're stopping
    if (context.isStopping()) {
      return;
    }

    localPerform();
  }

  abstract protected void localPerform() throws IOException;
}
