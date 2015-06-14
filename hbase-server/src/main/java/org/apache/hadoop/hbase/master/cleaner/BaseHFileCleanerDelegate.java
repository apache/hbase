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
package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Base class for the hfile cleaning function inside the master. By default, only the
 * {@link TimeToLiveHFileCleaner} is called.
 * <p>
 * If other effects are needed, implement your own LogCleanerDelegate and add it to the
 * configuration "hbase.master.hfilecleaner.plugins", which is a comma-separated list of fully
 * qualified class names. The <code>HFileCleaner</code> will build the cleaner chain in 
 * order the order specified by the configuration.
 * </p>
 * <p>
 * For subclasses, setConf will be called exactly <i>once</i> before using the cleaner.
 * </p>
 * <p>
 * Since {@link BaseHFileCleanerDelegate HFileCleanerDelegates} are created in
 * HFileCleaner by reflection, classes that implements this interface <b>must</b>
 * provide a default constructor.
 * </p>
 */
@InterfaceAudience.Private
public abstract class BaseHFileCleanerDelegate extends BaseFileCleanerDelegate {

  private boolean stopped = false;

  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}
