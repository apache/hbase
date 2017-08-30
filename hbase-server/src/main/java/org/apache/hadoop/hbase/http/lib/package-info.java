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
/**
 * <p>
 * This package provides user-selectable (via configuration) classes that add
 * functionality to the web UI. They are configured as a list of classes in the
 * configuration parameter <b>hadoop.http.filter.initializers</b>.
 * </p>
 * <ul>
 * <li> <b>StaticUserWebFilter</b> - An authorization plugin that makes all
 * users a static configured user.
 * </ul>
 * <p>
 * Copied from hadoop source code.<br>
 * See https://issues.apache.org/jira/browse/HADOOP-10232 to know why
 * </p>
 */
@InterfaceAudience.LimitedPrivate({"HBase"})
@InterfaceStability.Unstable
package org.apache.hadoop.hbase.http.lib;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
