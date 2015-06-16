/**
 *
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
package org.apache.hadoop.hbase.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * This coprocessor 'shallows' all the writes. It allows to test a pure
 * write workload, going through all the communication layers.
 * The reads will work as well, but they as we never write, they will always always
 * return an empty structure. The WAL is also skipped.
 * Obviously, the region will never be split automatically. It's up to the user
 * to split and move it.
 * </p>
 * <p>
 * For a table created like this:
 * create 'usertable', {NAME =&gt; 'f1', VERSIONS =&gt; 1}
 * </p>
 * <p>
 * You can then add the coprocessor with this command:
 * alter 'usertable', 'coprocessor' =&gt; '|org.apache.hadoop.hbase.tool.WriteSinkCoprocessor|'
 * </p>
 * <p>
 * And then
 * put 'usertable', 'f1', 'f1', 'f1'
 * </p>
 * <p>
 * scan 'usertable'
 * Will return:
 * 0 row(s) in 0.0050 seconds
 * </p>
 */
public class WriteSinkCoprocessor extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(WriteSinkCoprocessor.class);
  private final AtomicLong ops = new AtomicLong();
  private String regionName;

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    regionName = e.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString();
  }


  @Override
  public void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
                             final MiniBatchOperationInProgress<Mutation> miniBatchOp)
      throws IOException {
    if (ops.incrementAndGet() % 20000 == 0) {
      LOG.info("Wrote " + ops.get() + " times in region " + regionName);
    }

    for (int i = 0; i < miniBatchOp.size(); i++) {
      miniBatchOp.setOperationStatus(i,
          new OperationStatus(HConstants.OperationStatusCode.SUCCESS));
    }
    c.bypass();
  }
}
