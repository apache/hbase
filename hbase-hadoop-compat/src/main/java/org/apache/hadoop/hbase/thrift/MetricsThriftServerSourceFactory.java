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

import org.apache.yetus.audience.InterfaceAudience;

/** Factory that will be used to create metrics sources for the two diffent types of thrift servers. */
@InterfaceAudience.Private
public interface MetricsThriftServerSourceFactory {

  String METRICS_NAME = "Thrift";
  String METRICS_DESCRIPTION = "Thrift Server Metrics";
  String THRIFT_ONE_METRICS_CONTEXT = "thrift-one";
  String THRIFT_ONE_JMX_CONTEXT = "Thrift,sub=ThriftOne";
  String THRIFT_TWO_METRICS_CONTEXT = "thrift-two";
  String THRIFT_TWO_JMX_CONTEXT = "Thrift,sub=ThriftTwo";

  /** Create a Source for a thrift one server */
  MetricsThriftServerSource createThriftOneSource();

  /** Create a Source for a thrift two server */
  MetricsThriftServerSource createThriftTwoSource();

}
