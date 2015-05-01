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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * A factory for creating RegionMergeTransactions, which execute region split as a "transaction".
 * See {@link RegionMergeTransactionImpl}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class RegionMergeTransactionFactory implements Configurable {

  public static final String MERGE_TRANSACTION_IMPL_KEY =
      "hbase.regionserver.merge.transaction.impl";

  private Configuration conf;

  public RegionMergeTransactionFactory(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Create a merge transaction
   * @param a region a to merge
   * @param b region b to merge
   * @param forcible if false, we will only merge adjacent regions
   * @return transaction instance
   */
  public RegionMergeTransactionImpl create(final Region a, final Region b,
      final boolean forcible) {
    // The implementation class must extend RegionMergeTransactionImpl, not only
    // implement the RegionMergeTransaction interface like you might expect,
    // because various places such as AssignmentManager use static methods
    // from RegionMergeTransactionImpl. Whatever we use for implementation must
    // be compatible, so it's safest to require ? extends RegionMergeTransactionImpl.
    // If not compatible we will throw a runtime exception from here.
    return ReflectionUtils.instantiateWithCustomCtor(
      conf.getClass(MERGE_TRANSACTION_IMPL_KEY, RegionMergeTransactionImpl.class,
        RegionMergeTransactionImpl.class).getName(),
      new Class[] { Region.class, Region.class, boolean.class },
      new Object[] { a, b, forcible });
  }

}
