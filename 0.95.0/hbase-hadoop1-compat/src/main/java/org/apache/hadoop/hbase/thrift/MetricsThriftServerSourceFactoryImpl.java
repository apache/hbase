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

package org.apache.hadoop.hbase.thrift;

/**
 * Class used to create metrics sources for Thrift and Thrift2 servers in hadoop 1's compat
 * library.
 */
public class MetricsThriftServerSourceFactoryImpl implements MetricsThriftServerSourceFactory {

  /**
   * A singleton used to make sure that only one thrift metrics source per server type is ever
   * created.
   */
  private static enum FactoryStorage {
    INSTANCE;
    MetricsThriftServerSourceImpl thriftOne = new MetricsThriftServerSourceImpl(METRICS_NAME,
        METRICS_DESCRIPTION,
        THRIFT_ONE_METRICS_CONTEXT,
        THRIFT_ONE_JMX_CONTEXT);
    MetricsThriftServerSourceImpl thriftTwo = new MetricsThriftServerSourceImpl(METRICS_NAME,
        METRICS_DESCRIPTION,
        THRIFT_TWO_METRICS_CONTEXT,
        THRIFT_TWO_JMX_CONTEXT);
  }

  @Override
  public MetricsThriftServerSource createThriftOneSource() {
    return FactoryStorage.INSTANCE.thriftOne;
  }

  @Override
  public MetricsThriftServerSource createThriftTwoSource() {
    return FactoryStorage.INSTANCE.thriftTwo;
  }
}
