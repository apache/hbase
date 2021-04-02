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

package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ChaosZKClient {

  private static final Logger LOG = LoggerFactory.getLogger(ChaosZKClient.class.getName());
  private static final String CHAOS_AGENT_PARENT_ZNODE = "/hbase/chaosAgents";
  private static final String CHAOS_AGENT_STATUS_ZNODE = "/hbase/chaosAgentTaskStatus";
  private static final String ZNODE_PATH_SEPARATOR = "/";
  private static final String TASK_PREFIX = "task_";
  private static final String TASK_ERROR_STRING = "error";
  private static final String TASK_COMPLETION_STRING = "done";
  private static final String TASK_BOOLEAN_TRUE = "true";
  private static final String TASK_BOOLEAN_FALSE = "false";
  private static final String CONNECTION_LOSS = "ConnectionLoss";
  private static final int SESSION_TIMEOUT_ZK = 10 * 60 * 1000;
  private static final int TASK_EXECUTION_TIMEOUT = 5 * 60 * 1000;
  private volatile String taskStatus = null;

  private final String quorum;
  private ZooKeeper zk;

  public ChaosZKClient(String quorum) {
    this.quorum = quorum;
    try {
      this.createNewZKConnection();
    } catch (IOException e) {
      LOG.error("Error creating ZooKeeper Connection: ", e);
    }
  }

  /**
   * Creates connection with ZooKeeper
   * @throws IOException when not able to create connection properly
   */
  public void createNewZKConnection() throws IOException {
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        LOG.info("Created ZooKeeper Connection For executing task");
      }
    };

    this.zk = new ZooKeeper(quorum, SESSION_TIMEOUT_ZK, watcher);
  }

  /**
   * Checks if ChaosAgent is running or not on target host by checking its ZNode.
   * @param hostname hostname to check for chaosagent
   * @return true/false whether agent is running or not
   */
  private boolean isChaosAgentRunning(String hostname) {
    try {
      return zk.exists(CHAOS_AGENT_PARENT_ZNODE + ZNODE_PATH_SEPARATOR + hostname,
        false) != null;
    } catch (KeeperException e) {
      if (e.toString().contains(CONNECTION_LOSS)) {
        recreateZKConnection();
        try {
          return zk.exists(CHAOS_AGENT_PARENT_ZNODE + ZNODE_PATH_SEPARATOR + hostname,
            false) != null;
        } catch (KeeperException  | InterruptedException ie) {
          LOG.error("ERROR ", ie);
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Error checking for given hostname: {} ERROR: ", hostname, e);
    }
    return false;
  }

  /**
   * Creates tasks for target hosts by creating ZNodes.
   * Waits for a limited amount of time to complete task to execute.
   * @param taskObject Object data represents command
   * @return returns status
   */
  public String submitTask(final TaskObject taskObject) {
    if (isChaosAgentRunning(taskObject.getTaskHostname())) {
      LOG.info("Creating task node");
      zk.create(CHAOS_AGENT_STATUS_ZNODE + ZNODE_PATH_SEPARATOR +
          taskObject.getTaskHostname() + ZNODE_PATH_SEPARATOR + TASK_PREFIX,
        taskObject.getCommand().getBytes(),
        ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        submitTaskCallback,
        taskObject);
      long start = System.currentTimeMillis();

      while ((System.currentTimeMillis() - start) < TASK_EXECUTION_TIMEOUT) {
        if(taskStatus != null) {
          return taskStatus;
        }
        Threads.sleep(500);
      }
    } else {
      LOG.info("EHHHHH!  ChaosAgent Not running");
    }
    return TASK_ERROR_STRING;
  }

  /**
   * To get status of task submitted
   * @param path path at which to get status
   * @param ctx path context
   */
  private void getStatus(String path , Object ctx) {
    LOG.info("Getting Status of task: " + path);
    zk.getData(path,
      false,
      getStatusCallback,
      ctx);
  }

  /**
   * Set a watch on task submitted
   * @param name ZNode name to set a watch
   * @param taskObject context for ZNode name
   */
  private void setStatusWatch(String name, TaskObject taskObject) {
    LOG.info("Checking for ZNode and Setting watch for task : " + name);
    zk.exists(name,
      setStatusWatcher,
      setStatusWatchCallback,
      taskObject);
  }

  /**
   * Delete task after getting its status
   * @param path path to delete ZNode
   */
  private void deleteTask(String path) {
    LOG.info("Deleting task: " + path);
    zk.delete(path,
      -1,
      taskDeleteCallback,
      null);
  }

  //WATCHERS:

  /**
   * Watcher to get notification whenever status of task changes.
   */
  Watcher setStatusWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent watchedEvent) {
      LOG.info("Setting status watch for task: " + watchedEvent.getPath());
      if(watchedEvent.getType() == Event.EventType.NodeDataChanged) {
        if(!watchedEvent.getPath().contains(TASK_PREFIX)) {
          throw new RuntimeException(KeeperException.create(
            KeeperException.Code.DATAINCONSISTENCY));
        }
        getStatus(watchedEvent.getPath(), (Object) watchedEvent.getPath());

      }
    }
  };

  //CALLBACKS

  AsyncCallback.DataCallback getStatusCallback = (rc, path, ctx, data, stat) -> {
    switch (KeeperException.Code.get(rc)) {
      case CONNECTIONLOSS:
        //Connectionloss while getting status of task, getting again
        recreateZKConnection();
        getStatus(path, ctx);
        break;

      case OK:
        if (ctx!=null) {

          String status = new String(data);
          taskStatus = status;
          switch (status) {
            case TASK_COMPLETION_STRING:
            case TASK_BOOLEAN_TRUE:
            case TASK_BOOLEAN_FALSE:
              LOG.info("Task executed completely : Status --> " + status);
              break;

            case TASK_ERROR_STRING:
              LOG.info("There was error while executing task : Status --> " + status);
              break;

            default:
              LOG.warn("Status of task is undefined!! : Status --> " + status);
          }

          deleteTask(path);
        }
        break;

      default:
        LOG.error("ERROR while getting status of task: " + path + " ERROR: " +
          KeeperException.create(KeeperException.Code.get(rc)));
    }
  };

  AsyncCallback.StatCallback setStatusWatchCallback = (rc, path, ctx, stat) -> {
    switch (KeeperException.Code.get(rc)) {
      case CONNECTIONLOSS:
        //ConnectionLoss while setting watch on status ZNode, setting again.
        recreateZKConnection();
        setStatusWatch(path, (TaskObject) ctx);
        break;

      case OK:
        if(stat != null) {
          getStatus(path, null);
        }
        break;

      default:
        LOG.error("ERROR while setting watch on task ZNode: " + path + " ERROR: " +
          KeeperException.create(KeeperException.Code.get(rc)));
    }
  };

  AsyncCallback.StringCallback submitTaskCallback = (rc, path, ctx, name) -> {
    switch (KeeperException.Code.get(rc)) {
      case CONNECTIONLOSS:
        // Connection to server was lost while submitting task, submitting again.
        recreateZKConnection();
        submitTask((TaskObject) ctx);
        break;

      case OK:
        LOG.info("Task created : " + name);
        setStatusWatch(name, (TaskObject) ctx);
        break;

      default:
        LOG.error("Error submitting task: " + name + " ERROR:" +
          KeeperException.create(KeeperException.Code.get(rc)));
    }
  };

  AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx) {
      switch (KeeperException.Code.get(rc)) {
        case CONNECTIONLOSS:
          //Connectionloss while deleting task, deleting again
          recreateZKConnection();
          deleteTask(path);
          break;

        case OK:
          LOG.info("Task Deleted successfully!");
          LOG.info("Closing ZooKeeper Connection");
          try {
            zk.close();
          } catch (InterruptedException e) {
            LOG.error("Error while closing ZooKeeper Connection.");
          }
          break;

        default:
          LOG.error("ERROR while deleting task: " + path + " ERROR: " +
            KeeperException.create(KeeperException.Code.get(rc)));
      }
    }
  };


  private void recreateZKConnection() {
    try {
      zk.close();
    } catch (InterruptedException e) {
      LOG.error("Error closing ZK connection : ", e);
    } finally {
      try {
        createNewZKConnection();
      } catch (IOException e) {
        LOG.error("Error creating new ZK COnnection for agent: ", e);
      }
    }
  }

  static class TaskObject {
    private final String command;
    private final String taskHostname;

    public TaskObject(String command, String taskHostname) {
      this.command = command;
      this.taskHostname = taskHostname;
    }

    public String getCommand() {
      return this.command;
    }

    public String getTaskHostname() {
      return taskHostname;
    }
  }

}
