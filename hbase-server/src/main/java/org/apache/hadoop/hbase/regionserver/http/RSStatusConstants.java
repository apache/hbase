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
package org.apache.hadoop.hbase.regionserver.http;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Constants used by the web UI JSP pages.
 */
@InterfaceAudience.Private
public class RSStatusConstants {
  public static final String FILTER = "filter";
  public static final String FILTER_GENERAL = "general";
  public static final String FORMAT = "format";
  public static final String FORMAT_JSON = "json";
  public static final String FORMAT_HTML = "html";
  public static final String PARENT = "parent";
  public static final String BLOCK_CACHE_NAME = "bcn";
  public static final String BLOCK_CACHE_NAME_L1 = "L1";
  public static final String BLOCK_CACHE_V = "bcv";
  public static final String BLOCK_CACHE_V_FILE = "file";

  private RSStatusConstants() {
    // Do not instantiate.
  }
}
