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
 * A factory for creating SplitTransactions, which execute region split as a "transaction".
 * See {@link SplitTransaction}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class SplitTransactionFactory implements Configurable {

  public static final String SPLIT_TRANSACTION_IMPL_KEY =
      "hbase.regionserver.split.transaction.impl";

  private Configuration conf;

  public SplitTransactionFactory(Configuration conf) {
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
   * Create a split transaction
   * @param r the region to split
   * @param splitrow the split point in the keyspace
   * @return transaction instance
   */
  public SplitTransaction create(final Region r, final byte [] splitrow) {
    return ReflectionUtils.instantiateWithCustomCtor(
      // The implementation class must extend SplitTransactionImpl, not only
      // implement the SplitTransaction interface like you might expect,
      // because various places such as AssignmentManager use static methods
      // from SplitTransactionImpl. Whatever we use for implementation must
      // be compatible, so it's safest to require ? extends SplitTransactionImpl.
      // If not compatible we will throw a runtime exception from here.
      conf.getClass(SPLIT_TRANSACTION_IMPL_KEY, SplitTransactionImpl.class,
        SplitTransactionImpl.class).getName(),
      new Class[] { Region.class, byte[].class },
      new Object[] { r, splitrow });
  }

}
