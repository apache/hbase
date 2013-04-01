package org.apache.hadoop.hbase;
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

/**
 * Marker Interface used by ipc.  We need a means of referring to
 * ipc "protocols" generically.  For example, we need to tell an rpc
 * server the "protocols" it implements and it helps if all protocols
 * implement a common 'type'.  That is what this Interface is used for.
 */
// This Interface replaces the old VersionedProtocol Interface.  Rather
// than redo a bunch of code its removal, instead we put in place this
// Interface and change all VP references to Protocol references.

// It is moved up here to top-level because it is ugly having members
// of super packages reach down into subpackages.
public interface IpcProtocol {}
