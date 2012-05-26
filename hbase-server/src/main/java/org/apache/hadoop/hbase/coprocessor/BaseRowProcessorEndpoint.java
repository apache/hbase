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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RowProcessor;

/**
 * This class demonstrates how to implement atomic read-modify-writes
 * using {@link HRegion#processRowsWithLocks()} and Coprocessor endpoints.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class BaseRowProcessorEndpoint extends BaseEndpointCoprocessor
    implements RowProcessorProtocol {

  /**
   * Pass a processor to HRegion to process multiple rows atomically.
   * 
   * The RowProcessor implementations should be the inner classes of your
   * RowProcessorEndpoint. This way the RowProcessor can be class-loaded with
   * the Coprocessor endpoint together.
   *
   * See {@link TestRowProcessorEndpoint} for example.
   *
   * @param processor The object defines the read-modify-write procedure
   * @return The processing result
   */
  @Override
  public <T> T process(RowProcessor<T> processor)
      throws IOException {
    HRegion region =
        ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    region.processRowsWithLocks(processor);
    return processor.getResult();
  }

}
