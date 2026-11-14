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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

/*
 * Positional args for non-interactive scripts launched as:
 *   hbase path/to/script.jsh arg0 arg1 ...
 *
 * JShell has no String[] args equivalent, so bin/hbase exports:
 *   HBASE_JSH_ARG_COUNT  - number of args (decimal string)
 *   HBASE_JSH_ARG_0 .. HBASE_JSH_ARG_{N-1}  - each positional arg
 *
 * Example in a .jsh script:
 *   int n = Integer.parseInt(System.getenv().getOrDefault("HBASE_JSH_ARG_COUNT", "0"));
 *   String first = System.getenv("HBASE_JSH_ARG_0");
 */
