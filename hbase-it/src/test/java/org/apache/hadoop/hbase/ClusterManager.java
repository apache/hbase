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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configured;


/**
 * ClusterManager is an api to manage servers in a distributed environment. It provides services
 * for starting / stopping / killing Hadoop/HBase daemons. Concrete implementations provide actual
 * functionality for carrying out deployment-specific tasks.
 */
@InterfaceAudience.Private
public abstract class ClusterManager extends Configured {
  protected static final Log LOG = LogFactory.getLog(ClusterManager.class);

  private static final String SIGKILL = "SIGKILL";
  private static final String SIGSTOP = "SIGSTOP";
  private static final String SIGCONT = "SIGCONT";

  public ClusterManager() {
  }

  /**
   * Type of the service daemon
   */
  public static enum ServiceType {
    HADOOP_NAMENODE("namenode"),
    HADOOP_DATANODE("datanode"),
    HADOOP_JOBTRACKER("jobtracker"),
    HADOOP_TASKTRACKER("tasktracker"),
    HBASE_MASTER("master"),
    HBASE_REGIONSERVER("regionserver");

    private String name;

    ServiceType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  /**
   * Start the service on the given host
   */
  public abstract void start(ServiceType service, String hostname) throws IOException;

  /**
   * Stop the service on the given host
   */
  public abstract void stop(ServiceType service, String hostname) throws IOException;

  /**
   * Restart the service on the given host
   */
  public abstract void restart(ServiceType service, String hostname) throws IOException;

  /**
   * Send the given posix signal to the service
   */
  public abstract void signal(ServiceType service, String signal,
      String hostname) throws IOException;

  /**
   * Kill the service running on given host
   */
  public void kill(ServiceType service, String hostname) throws IOException {
    signal(service, SIGKILL, hostname);
  }

  /**
   * Suspend the service running on given host
   */
  public void suspend(ServiceType service, String hostname) throws IOException {
    signal(service, SIGSTOP, hostname);
  }

  /**
   * Resume the services running on given hosts
   */
  public void resume(ServiceType service, String hostname) throws IOException {
    signal(service, SIGCONT, hostname);
  }

  /**
   * Returns whether the service is running on the remote host. This only checks whether the
   * service still has a pid.
   */
  public abstract boolean isRunning(ServiceType service, String hostname) throws IOException;

  /* TODO: further API ideas:
   *
   * //return services running on host:
   * ServiceType[] getRunningServicesOnHost(String hostname);
   *
   * //return which services can be run on host (for example, to query whether hmaster can run on this host)
   * ServiceType[] getRunnableServicesOnHost(String hostname);
   *
   * //return which hosts can run this service
   * String[] getRunnableHostsForService(ServiceType service);
   */

}
