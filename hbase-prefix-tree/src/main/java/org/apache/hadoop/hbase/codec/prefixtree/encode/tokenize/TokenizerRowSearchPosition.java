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

package org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize;

import org.apache.hadoop.hbase.classification.InterfaceAudience;


/**
 * Warning: currently unused, but code is valid.  Pending performance testing on more data sets.
 *
 * Where is the key relative to our current position in the tree. For example, the current tree node
 * is "BEFORE" the key we are seeking
 */
@InterfaceAudience.Private
public enum TokenizerRowSearchPosition {

	AFTER,//the key is after this tree node, so keep searching
	BEFORE,//in a binary search, this tells us to back up
	MATCH,//the current node is a full match
	NO_MATCH,//might as well return a value more informative than null

}
