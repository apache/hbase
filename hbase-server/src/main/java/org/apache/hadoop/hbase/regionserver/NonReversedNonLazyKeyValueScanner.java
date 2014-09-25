/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;

/**
 * A "non-reversed & non-lazy" scanner which does not support backward scanning
 * and always does a real seek operation. Most scanners are inherited from this
 * class.
 */
@InterfaceAudience.Private
public abstract class NonReversedNonLazyKeyValueScanner extends
    NonLazyKeyValueScanner {

  @Override
  public boolean backwardSeek(KeyValue key) throws IOException {
    throw new NotImplementedException("backwardSeek must not be called on a "
        + "non-reversed scanner");
  }

  @Override
  public boolean seekToPreviousRow(KeyValue key) throws IOException {
    throw new NotImplementedException("seekToPreviousRow must not be called on a "
        + "non-reversed scanner");
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    throw new NotImplementedException("seekToLastRow must not be called on a "
        + "non-reversed scanner");
  }

}
