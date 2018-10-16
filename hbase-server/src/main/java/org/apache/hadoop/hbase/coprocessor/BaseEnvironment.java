/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Encapsulation of the environment of each coprocessor
 */
@InterfaceAudience.Private
public class BaseEnvironment<C extends Coprocessor> implements CoprocessorEnvironment<C> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseEnvironment.class);

  /** The coprocessor */
  public C impl;
  /** Chaining priority */
  protected int priority = Coprocessor.PRIORITY_USER;
  /** Current coprocessor state */
  Coprocessor.State state = Coprocessor.State.UNINSTALLED;
  private int seq;
  private Configuration conf;
  private ClassLoader classLoader;

  /**
   * Constructor
   * @param impl the coprocessor instance
   * @param priority chaining priority
   */
  public BaseEnvironment(final C impl, final int priority, final int seq, final Configuration conf) {
    this.impl = impl;
    this.classLoader = impl.getClass().getClassLoader();
    this.priority = priority;
    this.state = Coprocessor.State.INSTALLED;
    this.seq = seq;
    this.conf = new ReadOnlyConfiguration(conf);
  }

  /** Initialize the environment */
  public void startup() throws IOException {
    if (state == Coprocessor.State.INSTALLED ||
        state == Coprocessor.State.STOPPED) {
      state = Coprocessor.State.STARTING;
      Thread currentThread = Thread.currentThread();
      ClassLoader hostClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(this.getClassLoader());
        impl.start(this);
        state = Coprocessor.State.ACTIVE;
      } finally {
        currentThread.setContextClassLoader(hostClassLoader);
      }
    } else {
      LOG.warn("Not starting coprocessor " + impl.getClass().getName() +
          " because not inactive (state=" + state.toString() + ")");
    }
  }

  /** Clean up the environment */
  public void shutdown() {
    if (state == Coprocessor.State.ACTIVE) {
      state = Coprocessor.State.STOPPING;
      Thread currentThread = Thread.currentThread();
      ClassLoader hostClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(this.getClassLoader());
        impl.stop(this);
        state = Coprocessor.State.STOPPED;
      } catch (IOException ioe) {
        LOG.error("Error stopping coprocessor "+impl.getClass().getName(), ioe);
      } finally {
        currentThread.setContextClassLoader(hostClassLoader);
      }
    } else {
      LOG.warn("Not stopping coprocessor "+impl.getClass().getName()+
          " because not active (state="+state.toString()+")");
    }
  }

  @Override
  public C getInstance() {
    return impl;
  }

  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Override
  public int getPriority() {
    return priority;
  }

  @Override
  public int getLoadSequence() {
    return seq;
  }

  /** @return the coprocessor environment version */
  @Override
  public int getVersion() {
    return Coprocessor.VERSION;
  }

  /** @return the HBase release */
  @Override
  public String getHBaseVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }
}
