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
 * for recursively searching a PtBuilder
 */
@InterfaceAudience.Private
public class TokenizerRowSearchResult{

  /************ fields ************************/

  protected TokenizerRowSearchPosition difference;
  protected TokenizerNode matchingNode;


  /*************** construct *****************/

  public TokenizerRowSearchResult() {
  }

  public TokenizerRowSearchResult(TokenizerRowSearchPosition difference) {
    this.difference = difference;
  }

  public TokenizerRowSearchResult(TokenizerNode matchingNode) {
    this.difference = TokenizerRowSearchPosition.MATCH;
    this.matchingNode = matchingNode;
  }


  /*************** methods **********************/

  public boolean isMatch() {
    return TokenizerRowSearchPosition.MATCH == difference;
  }


  /************* get/set ***************************/

  public TokenizerRowSearchPosition getDifference() {
    return difference;
  }

  public TokenizerNode getMatchingNode() {
    return matchingNode;
  }

  public void set(TokenizerRowSearchPosition difference, TokenizerNode matchingNode) {
    this.difference = difference;
    this.matchingNode = matchingNode;
  }

}
