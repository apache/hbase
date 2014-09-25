/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * An abstract class that implements RegionServerObserver.
 * By extending it, you can create your own region server observer without
 * overriding all abstract methods of RegionServerObserver.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class BaseRegionServerObserver implements RegionServerObserver {

  @Override
  public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
      throws IOException { }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException { }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException { }

  @Override
  public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, HRegion regionA,
      HRegion regionB) throws IOException { }

  @Override
  public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, HRegion regionA,
      HRegion regionB, HRegion mergedRegion) throws IOException { }

  @Override
  public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      HRegion regionA, HRegion regionB, List<Mutation> metaEntries) throws IOException { }

  @Override
  public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      HRegion regionA, HRegion regionB, HRegion mergedRegion) throws IOException { }

  @Override
  public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      HRegion regionA, HRegion regionB) throws IOException { }

  @Override
  public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      HRegion regionA, HRegion regionB) throws IOException { }

  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException { }

  @Override
  public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException { }

}
