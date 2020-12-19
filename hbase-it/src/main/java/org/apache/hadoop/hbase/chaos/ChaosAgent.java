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

package org.apache.hadoop.hbase.chaos;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.util.Shell;
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

/***
 * An agent for executing destructive actions for ChaosMonkey.
 * Uses ZooKeeper Watchers and LocalShell, to do the killing
 * and getting status of service on targeted host without SSH.
 * uses given ZNode Structure:
 *  /perfChaosTest (root)
 *              |
 *              |
 *              /chaosAgents (Used for registration has
 *              hostname ephemeral nodes as children)
 *              |
 *              |
 *              /chaosAgentTaskStatus (Used for task
 *              Execution, has hostname persistent
 *              nodes as child with tasks as their children)
 *                          |
 *                          |
 *                          /hostname
 *                                |
 *                                |
 *                                /task0000001 (command as data)
 *                                (has two types of command :
 *                                     1: starts with "exec"
 *                                       for executing a destructive action.
 *                                     2: starts with "bool" for getting
 *                                       only status of service.
 *
 */
@InterfaceAudience.Private
public class ChaosAgent implements Watcher, Closeable, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ChaosAgent.class);
  static AtomicBoolean stopChaosAgent = new AtomicBoolean();
  private ZooKeeper zk;
  private String quorum;
  private String agentName;
  private Configuration conf;
  private RetryCounterFactory retryCounterFactory;
  private volatile boolean connected = false;

  public ChaosAgent(Configuration conf, String quorum, String agentName) {
    initChaosAgent(conf, quorum, agentName);
  }

  /***
   * sets global params and initiates connection with ZooKeeper then does registration.
   * @param conf initial configuration to use
   * @param quorum ZK Quorum
   * @param agentName AgentName to use
   */
  private void initChaosAgent(Configuration conf, String quorum, String agentName) {
    this.conf = conf;
    this.quorum = quorum;
    this.agentName = agentName;
    this.retryCounterFactory = new RetryCounterFactory(new RetryCounter.RetryConfig()
      .setMaxAttempts(conf.getInt(ChaosConstants.RETRY_ATTEMPTS_KEY,
        ChaosConstants.DEFAULT_RETRY_ATTEMPTS)).setSleepInterval(
          conf.getLong(ChaosConstants.RETRY_SLEEP_INTERVAL_KEY,
            ChaosConstants.DEFAULT_RETRY_SLEEP_INTERVAL)));
    try {
      this.createZKConnection(null);
      this.register();
    } catch (IOException e) {
      LOG.error("Error Creating Connection: " + e);
    }
  }

  /***
   * Creates Connection with ZooKeeper.
   * @throws IOException if something goes wrong
   */
  private void createZKConnection(Watcher watcher) throws IOException {
    if(watcher == null) {
      zk = new ZooKeeper(quorum, ChaosConstants.SESSION_TIMEOUT_ZK, this);
    } else {
      zk = new ZooKeeper(quorum, ChaosConstants.SESSION_TIMEOUT_ZK, watcher);
    }
    LOG.info("ZooKeeper Connection created for ChaosAgent: " + agentName);
  }

  //WATCHERS: Below are the Watches used by ChaosAgent

  /***
   * Watcher for notifying if any task is assigned to agent or not,
   * by seeking if any Node is being added to agent as Child.
   */
  Watcher newTaskCreatedWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent watchedEvent) {
      if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
        if (!(ChaosConstants.CHAOS_AGENT_STATUS_PERSISTENT_ZNODE +
          ChaosConstants.ZNODE_PATH_SEPARATOR + agentName).equals(watchedEvent.getPath())) {
          throw new RuntimeException(KeeperException.create(
            KeeperException.Code.DATAINCONSISTENCY));
        }

        LOG.info("Change in Tasks Node, checking for Tasks again.");
        getTasks();
      }

    }
  };

  //CALLBACKS: Below are the Callbacks used by Chaos Agent

  /**
   * Callback used while setting status of a given task, Logs given status.
   */
  AsyncCallback.StatCallback setStatusOfTaskZNodeCallback = (rc, path, ctx, stat) -> {
    switch (KeeperException.Code.get(rc)) {
      case CONNECTIONLOSS:
        // Connection to the server was lost while setting status setting again.
        try {
          recreateZKConnection();
        } catch (Exception e) {
          break;
        }
        setStatusOfTaskZNode(path, (String) ctx);
        break;

      case OK:
        LOG.info("Status of Task has been set");
        break;

      case NONODE:
        LOG.error("Chaos Agent status node does not exists: "
          + "check for ZNode directory structure again.");
        break;

      default:
        LOG.error("Error while setting status of task ZNode: " +
          path, KeeperException.create(KeeperException.Code.get(rc), path));
    }
  };

  /**
   * Callback used while creating a Persistent ZNode tries to create
   * ZNode again if Connection was lost in previous try.
   */
  AsyncCallback.StringCallback createZNodeCallback = (rc, path, ctx, name) -> {
    switch (KeeperException.Code.get(rc)) {
      case CONNECTIONLOSS:
        try {
          recreateZKConnection();
        } catch (Exception e) {
          break;
        }
        createZNode(path, (byte[]) ctx);
        break;
      case OK:
        LOG.info("ZNode created : " + path);
        break;
      case NODEEXISTS:
        LOG.warn("ZNode already registered: " + path);
        break;
      default:
        LOG.error("Error occurred while creating Persistent ZNode: " + path,
          KeeperException.create(KeeperException.Code.get(rc), path));
    }
  };

  /**
   * Callback used while creating a Ephemeral ZNode tries to create ZNode again
   * if Connection was lost in previous try.
   */
  AsyncCallback.StringCallback createEphemeralZNodeCallback = (rc, path, ctx, name) -> {
    switch (KeeperException.Code.get(rc)) {
      case CONNECTIONLOSS:
        try {
          recreateZKConnection();
        } catch (Exception e) {
          break;
        }
        createEphemeralZNode(path, (byte[]) ctx);
        break;
      case OK:
        LOG.info("ZNode created : " + path);
        break;
      case NODEEXISTS:
        LOG.warn("ZNode already registered: " + path);
        break;
      default:
        LOG.error("Error occurred while creating Ephemeral ZNode: ",
          KeeperException.create(KeeperException.Code.get(rc), path));
    }
  };

  /**
   * Callback used by getTasksForAgentCallback while getting command,
   * after getting command successfully, it executes command and
   * set its status with respect to the command type.
   */
  AsyncCallback.DataCallback getTaskForExecutionCallback = new AsyncCallback.DataCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      switch (KeeperException.Code.get(rc)) {
        case CONNECTIONLOSS:
          //Connection to the server has been lost while getting task, getting data again.
          try {
            recreateZKConnection();
          } catch (Exception e) {
            break;
          }
          zk.getData(path,
            false,
            getTaskForExecutionCallback,
            new String(data));
          break;
        case OK:
          String cmd = new String(data);
          LOG.info("Executing command : " + cmd);
          String status = ChaosConstants.TASK_COMPLETION_STRING;
          try {
            String user = conf.get(ChaosConstants.CHAOSAGENT_SHELL_USER,
              ChaosConstants.DEFAULT_SHELL_USER);
            switch (cmd.substring(0, 4)) {
              case "bool":
                String ret = execWithRetries(user, cmd.substring(4)).getSecond();
                status = Boolean.toString(ret.length() > 0);
                break;

              case "exec":
                execWithRetries(user, cmd.substring(4));
                break;

              default:
                LOG.error("Unknown Command Type");
                status = ChaosConstants.TASK_ERROR_STRING;
            }
          } catch (IOException e) {
            LOG.error("Got error while executing command : " + cmd +
              " On agent : " + agentName + " Error : " + e);
            status = ChaosConstants.TASK_ERROR_STRING;
          }

          try {
            setStatusOfTaskZNode(path, status);
            Thread.sleep(ChaosConstants.SET_STATUS_SLEEP_TIME);
          } catch (InterruptedException e) {
            LOG.error("Error occured after setting status: " + e);
          }

        default:
          LOG.error("Error occurred while getting data",
            KeeperException.create(KeeperException.Code.get(rc), path));
      }
    }
  };

  /***
   * Callback used while getting Tasks for agent if call executed without Exception,
   * It creates a separate thread for each children to execute given Tasks parallely.
   */
  AsyncCallback.ChildrenCallback getTasksForAgentCallback = new AsyncCallback.ChildrenCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      switch (KeeperException.Code.get(rc)) {
        case CONNECTIONLOSS: {
          // Connection to the server has been lost, getting tasks again.
          try {
            recreateZKConnection();
          } catch (Exception e) {
            break;
          }
          getTasks();
          break;
        }

        case OK: {
          if (children != null) {
            try {

              LOG.info("Executing each task as a separate thread");
              List<Thread> tasksList = new ArrayList<>();
              for (String task : children) {
                String threadName = agentName + "_" + task;
                Thread t = new Thread(() -> {

                  LOG.info("Executing task : " + task + " of agent : " + agentName);
                  zk.getData(ChaosConstants.CHAOS_AGENT_STATUS_PERSISTENT_ZNODE +
                      ChaosConstants.ZNODE_PATH_SEPARATOR + agentName +
                      ChaosConstants.ZNODE_PATH_SEPARATOR + task,
                    false,
                    getTaskForExecutionCallback,
                    task);

                });
                t.setName(threadName);
                t.start();
                tasksList.add(t);

                for (Thread thread : tasksList) {
                  thread.join();
                }
              }
            } catch (InterruptedException e) {
              LOG.error("Error scheduling next task : " +
                " for agent : " + agentName + " Error : " + e);
            }
          }
          break;
        }

        default:
          LOG.error("Error occurred while getting task",
            KeeperException.create(KeeperException.Code.get(rc), path));
      }
    }
  };

  /***
   * Function to create PERSISTENT ZNODE with given path and data given as params
   * @param path Path at which ZNode to create
   * @param data Data to put under ZNode
   */
  public void createZNode(String path, byte[] data) {
    zk.create(path,
      data,
      ZooDefs.Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT,
      createZNodeCallback,
      data);
  }

  /***
   * Function to create EPHEMERAL ZNODE with given path and data as params.
   * @param path Path at which Ephemeral ZNode to create
   * @param data Data to put under ZNode
   */
  public void createEphemeralZNode(String path, byte[] data) {
    zk.create(path,
      data,
      ZooDefs.Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL,
      createEphemeralZNodeCallback,
      data);
  }

  /**
   * Checks if given ZNode exists, if not creates a PERSISTENT ZNODE for same.
   *
   * @param path Path to check for ZNode
   */
  private void createIfZNodeNotExists(String path) {
    try {
      if (zk.exists(path,
        false) == null) {
        createZNode(path, new byte[0]);
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error checking given node : " + path + " " + e);
    }
  }

  /**
   * sets given Status for Task Znode
   *
   * @param taskZNode ZNode to set status
   * @param status Status value
   */
  public void setStatusOfTaskZNode(String taskZNode, String status) {
    LOG.info("Setting status of Task ZNode: " + taskZNode + " status : " + status);
    zk.setData(taskZNode,
      status.getBytes(),
      -1,
      setStatusOfTaskZNodeCallback,
      null);
  }

  /**
   * registration of ChaosAgent by checking and creating necessary ZNodes.
   */
  private void register() {
    createIfZNodeNotExists(ChaosConstants.CHAOS_TEST_ROOT_ZNODE);
    createIfZNodeNotExists(ChaosConstants.CHAOS_AGENT_REGISTRATION_EPIMERAL_ZNODE);
    createIfZNodeNotExists(ChaosConstants.CHAOS_AGENT_STATUS_PERSISTENT_ZNODE);
    createIfZNodeNotExists(ChaosConstants.CHAOS_AGENT_STATUS_PERSISTENT_ZNODE +
      ChaosConstants.ZNODE_PATH_SEPARATOR + agentName);

    createEphemeralZNode(ChaosConstants.CHAOS_AGENT_REGISTRATION_EPIMERAL_ZNODE +
      ChaosConstants.ZNODE_PATH_SEPARATOR + agentName, new byte[0]);
  }

  /***
   * Gets tasks for execution, basically sets Watch on it's respective host's Znode and
   * waits for tasks to be assigned, also has a getTasksForAgentCallback
   * which handles execution of task.
   */
  private void getTasks() {
    LOG.info("Getting Tasks for Agent: " + agentName + "and setting watch for new Tasks");
    zk.getChildren(ChaosConstants.CHAOS_AGENT_STATUS_PERSISTENT_ZNODE +
        ChaosConstants.ZNODE_PATH_SEPARATOR + agentName,
      newTaskCreatedWatcher,
      getTasksForAgentCallback,
      null);
  }

  /**
   * Below function executes command with retries with given user.
   * Uses LocalShell to execute a command.
   *
   * @param user user name, default none
   * @param cmd Command to execute
   * @return A pair of Exit Code and Shell output
   * @throws IOException Exception while executing shell command
   */
  private Pair<Integer, String> execWithRetries(String user, String cmd) throws IOException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return exec(user, cmd);
      } catch (IOException e) {
        retryOrThrow(retryCounter, e, user, cmd);
      }
      try {
        retryCounter.sleepUntilNextRetry();
      } catch (InterruptedException e) {
        LOG.warn("Sleep Interrupted: " + e);
      }
    }
  }

  private Pair<Integer, String> exec(String user, String cmd) throws IOException {
    LOG.info("Executing Shell command: " + cmd + " , user: " + user);

    LocalShell shell = new LocalShell(user, cmd);
    try {
      shell.execute();
    } catch (Shell.ExitCodeException e) {
      String output = shell.getOutput();
      throw new Shell.ExitCodeException(e.getExitCode(), "stderr: " + e.getMessage()
        + ", stdout: " + output);
    }
    LOG.info("Executed Shell command, exit code: {}, output n{}", shell.getExitCode(), shell.getOutput());

    return new Pair<>(shell.getExitCode(), shell.getOutput());
  }

  private <E extends Exception> void retryOrThrow(RetryCounter retryCounter, E ex,
    String user, String cmd) throws E {
    if (retryCounter.shouldRetry()) {
      LOG.warn("Local command: {}, user: {}, failed at attempt {}. Retrying until maxAttempts: {}."
        + "Exception {}", cmd, user,retryCounter.getAttemptTimes(), retryCounter.getMaxAttempts(),
        ex.getMessage());
      return;
    }
    throw ex;
  }

  private boolean isConnected() {
    return connected;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing ZooKeeper Connection for Chaos Agent : " + agentName);
    try {
      zk.close();
    } catch (InterruptedException e) {
      LOG.error("Error while closing ZooKeeper Connection.");
    }
  }

  @Override
  public void run() {
    try {
      LOG.info("Running Chaos Agent on : " + agentName);
      while (!this.isConnected()) {
        Thread.sleep(100);
      }
      this.getTasks();
      while (!stopChaosAgent.get()) {
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      LOG.error("Error while running Chaos Agent", e);
    }

  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    LOG.info("Processing event: " + watchedEvent.toString());
    if (watchedEvent.getType() == Event.EventType.None) {
      switch (watchedEvent.getState()) {
        case SyncConnected:
          connected = true;
          break;
        case Disconnected:
          connected = false;
          break;
        case Expired:
          connected = false;
          LOG.error("Session expired creating again");
          try {
            createZKConnection(null);
          } catch (IOException e) {
            LOG.error("Error creating Zookeeper connection", e);
          }
        default:
          LOG.error("Unknown State");
          break;
      }
    }
  }

  private void recreateZKConnection() throws Exception{
    try {
      zk.close();
        createZKConnection(newTaskCreatedWatcher);
        createEphemeralZNode(ChaosConstants.CHAOS_AGENT_REGISTRATION_EPIMERAL_ZNODE +
          ChaosConstants.ZNODE_PATH_SEPARATOR + agentName, new byte[0]);
      } catch (IOException e) {
        LOG.error("Error creating new ZK COnnection for agent: {}", agentName + e);
        throw e;
      }
    }

  /**
   * Executes Command locally.
   */
  protected static class LocalShell extends Shell.ShellCommandExecutor {

    private String user;
    private String execCommand;

    public LocalShell(String user, String execCommand) {
      super(new String[]{execCommand});
      this.user = user;
      this.execCommand = execCommand;
    }

    @Override
    public String[] getExecString() {
      // TODO: Considering Agent is running with same user.
      if(!user.equals(ChaosConstants.DEFAULT_SHELL_USER)){
        execCommand = String.format("su -u %1$s %2$s", user, execCommand);
      }
      return new String[]{"/usr/bin/env", "bash", "-c", execCommand};
    }

    @Override
    public void execute() throws IOException {
      super.execute();
    }
  }
}
