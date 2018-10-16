/*
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * A factory class that constructs an {@link org.apache.hadoop.hbase.client.RequestController}.
 */
@InterfaceAudience.Public
public final class RequestControllerFactory {
  public static final String REQUEST_CONTROLLER_IMPL_CONF_KEY = "hbase.client.request.controller.impl";
  /**
   * Constructs a {@link org.apache.hadoop.hbase.client.RequestController}.
   * @param conf The {@link Configuration} to use.
   * @return A RequestController which is built according to the configuration.
   */
  public static RequestController create(Configuration conf) {
    Class<? extends RequestController> clazz= conf.getClass(REQUEST_CONTROLLER_IMPL_CONF_KEY,
      SimpleRequestController.class, RequestController.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }
}
