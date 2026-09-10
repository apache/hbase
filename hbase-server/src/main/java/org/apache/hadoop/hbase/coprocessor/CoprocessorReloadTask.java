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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This interface is used to perform whatever task is necessary for reloading coprocessors on
 * HMaster, HRegionServer, and HRegion. Since the steps required to reload coprocessors varies for
 * each of these types, this interface helps with code flexibility by allowing a lamda function to
 * be provided for the {@link #reload(Configuration) reload()} method. <br>
 * <br>
 * See {@link org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil#maybeUpdateCoprocessors
 * CoprocessorConfigurationUtil.maybeUpdateCoprocessors()} and its usage in
 * {@link org.apache.hadoop.hbase.conf.ConfigurationObserver#onConfigurationChange
 * onConfigurationChange()} with HMaster, HRegionServer, and HRegion for an idea of how this
 * interface is helpful.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
@FunctionalInterface
public interface CoprocessorReloadTask {
  void reload(Configuration conf);
}
