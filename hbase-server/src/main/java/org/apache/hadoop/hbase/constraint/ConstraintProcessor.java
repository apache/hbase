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
package org.apache.hadoop.hbase.constraint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/***
 * Processes multiple {@link Constraint Constraints} on a given table.
 * <p>
 * This is an ease of use mechanism - all the functionality here could be
 * implemented on any given system by a coprocessor.
 */
@InterfaceAudience.Private
public class ConstraintProcessor extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(ConstraintProcessor.class);

  private final ClassLoader classloader;

  private List<? extends Constraint> constraints = new ArrayList<Constraint>();

  /**
   * Create the constraint processor.
   * <p>
   * Stores the current classloader.
   */
  public ConstraintProcessor() {
    classloader = this.getClass().getClassLoader();
  }

  @Override
  public void start(CoprocessorEnvironment environment) {
    // make sure we are on a region server
    if (!(environment instanceof RegionCoprocessorEnvironment)) {
      throw new IllegalArgumentException(
          "Constraints only act on regions - started in an environment that was not a region");
    }
    RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) environment;
    HTableDescriptor desc = env.getRegion().getTableDesc();
    // load all the constraints from the HTD
    try {
      this.constraints = Constraints.getConstraints(desc, classloader);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Finished loading " + constraints.size()
          + " user Constraints on table: " + desc.getTableName());
    }

  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, Durability durability) throws IOException {
    // check the put against the stored constraints
    for (Constraint c : constraints) {
      c.check(put);
    }
    // if we made it here, then the Put is valid
  }

  @Override
  public boolean postScannerFilterRow(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final Cell curRowCell, final boolean hasMore) throws IOException {
    // Impl in BaseRegionObserver might do unnecessary copy for Off heap backed Cells.
    return hasMore;
  }
}
