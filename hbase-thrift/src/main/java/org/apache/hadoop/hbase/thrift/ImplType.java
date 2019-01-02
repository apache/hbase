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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.Constants.SERVER_TYPE_CONF_KEY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.OptionGroup;

/** An enum of server implementation selections */
@InterfaceAudience.Private
public enum ImplType {
  HS_HA("hsha", true, THsHaServer.class, true),
  NONBLOCKING("nonblocking", true, TNonblockingServer.class, true),
  THREAD_POOL("threadpool", false, TBoundedThreadPoolServer.class, true),
  THREADED_SELECTOR("threadedselector", true, TThreadedSelectorServer.class, true);

  private static final Logger LOG = LoggerFactory.getLogger(ImplType.class);
  public static final ImplType DEFAULT = THREAD_POOL;


  final String option;
  final boolean isAlwaysFramed;
  final Class<? extends TServer> serverClass;
  final boolean canSpecifyBindIP;

  private ImplType(String option, boolean isAlwaysFramed,
      Class<? extends TServer> serverClass, boolean canSpecifyBindIP) {
    this.option = option;
    this.isAlwaysFramed = isAlwaysFramed;
    this.serverClass = serverClass;
    this.canSpecifyBindIP = canSpecifyBindIP;
  }

  /**
   * @return <code>-option</code>
   */
  @Override
  public String toString() {
    return "-" + option;
  }

  public String getOption() {
    return option;
  }

  public boolean isAlwaysFramed() {
    return isAlwaysFramed;
  }

  public String getDescription() {
    StringBuilder sb = new StringBuilder("Use the " +
        serverClass.getSimpleName());
    if (isAlwaysFramed) {
      sb.append(" This implies the framed transport.");
    }
    if (this == DEFAULT) {
      sb.append("This is the default.");
    }
    return sb.toString();
  }

  static OptionGroup createOptionGroup() {
    OptionGroup group = new OptionGroup();
    for (ImplType t : values()) {
      group.addOption(new Option(t.option, t.getDescription()));
    }
    return group;
  }

  public static ImplType getServerImpl(Configuration conf) {
    String confType = conf.get(SERVER_TYPE_CONF_KEY, THREAD_POOL.option);
    for (ImplType t : values()) {
      if (confType.equals(t.option)) {
        return t;
      }
    }
    throw new AssertionError("Unknown server ImplType.option:" + confType);
  }

  static void setServerImpl(CommandLine cmd, Configuration conf) {
    ImplType chosenType = null;
    int numChosen = 0;
    for (ImplType t : values()) {
      if (cmd.hasOption(t.option)) {
        chosenType = t;
        ++numChosen;
      }
    }
    if (numChosen < 1) {
      LOG.info("Using default thrift server type");
      chosenType = DEFAULT;
    } else if (numChosen > 1) {
      throw new AssertionError("Exactly one option out of " +
          Arrays.toString(values()) + " has to be specified");
    }
    LOG.info("Using thrift server type " + chosenType.option);
    conf.set(SERVER_TYPE_CONF_KEY, chosenType.option);
  }

  public String simpleClassName() {
    return serverClass.getSimpleName();
  }

  public static List<String> serversThatCannotSpecifyBindIP() {
    List<String> l = new ArrayList<>();
    for (ImplType t : values()) {
      if (!t.canSpecifyBindIP) {
        l.add(t.simpleClassName());
      }
    }
    return l;
  }
}
