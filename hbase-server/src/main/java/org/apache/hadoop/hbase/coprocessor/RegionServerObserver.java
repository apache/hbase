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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;

public interface RegionServerObserver extends Coprocessor {

  /**
   * Called before stopping region server.
   * @param env An instance of RegionServerCoprocessorEnvironment
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void preStopRegionServer(
    final ObserverContext<RegionServerCoprocessorEnvironment> env)
    throws IOException;

  /**
   * Called before the regions merge. 
   * Call {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} to skip the merge.
   * @throws IOException if an error occurred on the coprocessor
   * @param ctx
   * @param regionA
   * @param regionB
   * @throws IOException
   */
  void preMerge(final ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      final HRegion regionA, final HRegion regionB) throws IOException;

  /**
   * called after the regions merge.
   * @param c
   * @param regionA
   * @param regionB
   * @param mergedRegion
   * @throws IOException
   */
  void postMerge(final ObserverContext<RegionServerCoprocessorEnvironment> c,
      final HRegion regionA, final HRegion regionB, final HRegion mergedRegion) throws IOException;

  /**
   * This will be called before PONR step as part of regions merge transaction. Calling
   * {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} rollback the merge
   * @param ctx
   * @param regionA
   * @param regionB
   * @param metaEntries mutations to execute on hbase:meta atomically with regions merge updates. 
   *        Any puts or deletes to execute on hbase:meta can be added to the mutations.
   * @throws IOException
   */
  void preMergeCommit(final ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      final HRegion regionA, final HRegion regionB,
      @MetaMutationAnnotation List<Mutation> metaEntries) throws IOException;

  /**
   * This will be called after PONR step as part of regions merge transaction.
   * @param ctx
   * @param regionA
   * @param regionB
   * @param mergedRegion
   * @throws IOException
   */
  void postMergeCommit(final ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      final HRegion regionA, final HRegion regionB, final HRegion mergedRegion) throws IOException;

  /**
   * This will be called before the roll back of the regions merge.
   * @param ctx
   * @param regionA
   * @param regionB
   * @throws IOException
   */
  void preRollBackMerge(final ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      final HRegion regionA, final HRegion regionB) throws IOException;

  /**
   * This will be called after the roll back of the regions merge.
   * @param ctx
   * @param regionA
   * @param regionB
   * @throws IOException
   */
  void postRollBackMerge(final ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      final HRegion regionA, final HRegion regionB) throws IOException;

  /**
   * This will be called before executing user request to roll a region server WAL.
   * @param ctx An instance of ObserverContext
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void preRollWALWriterRequest(final ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException;

  /**
   * This will be called after executing user request to roll a region server WAL.
   * @param ctx An instance of ObserverContext
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void postRollWALWriterRequest(final ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException;

}
