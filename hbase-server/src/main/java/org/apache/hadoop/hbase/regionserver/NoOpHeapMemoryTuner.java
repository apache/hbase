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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerContext;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerResult;

/**
 * An implementation of HeapMemoryTuner which is not doing any tuning activity but just allows to
 * continue with old style fixed proportions.
 */
@InterfaceAudience.Private
public class NoOpHeapMemoryTuner implements HeapMemoryTuner {
  
  private static final TunerResult NO_OP_TUNER_RESULT = new TunerResult(false);

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void setConf(Configuration conf) {

  }

  @Override
  public TunerResult tune(TunerContext context) {
    return NO_OP_TUNER_RESULT;
  }
}
