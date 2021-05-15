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
 * This package should be in the hbase-http module as {@code a.a.h.h.http.gson}. It is here instead
 * because hbase-http does not currently have a dependency on hbase-client, which is required for
 * implementing {@link org.apache.hadoop.hbase.master.http.gson.SizeAsBytesSerializer}.
 */
package org.apache.hadoop.hbase.master.http.gson;
