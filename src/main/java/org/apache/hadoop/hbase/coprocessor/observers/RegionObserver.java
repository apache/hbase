/*
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

package org.apache.hadoop.hbase.coprocessor.observers;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.environments.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Coprocessors implement this interface to observe and mediate client actions
 * on the region.
 * TODO: this contains just a handful of calls - we should extend this vastly in future
 */
public interface RegionObserver extends Coprocessor {

  /**
   * Called before the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param put The Put object
   * @param edit The WALEdit object that will be written to the wal
   * @param durability Persistence guarantee for this Put
   * @throws IOException if an error occurred on the coprocessor
   */
  void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final boolean writeToWAL)
    throws IOException;

  /**
   * Called after the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param put The Put object
   * @param edit The WALEdit object for the wal
   * @param durability Persistence guarantee for this Put
   * @throws IOException if an error occurred on the coprocessor
   */
  void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final boolean writeToWAL)
    throws IOException;

}
