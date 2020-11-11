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

package org.apache.hadoop.hbase.namequeues;

import com.lmax.disruptor.ExceptionHandler;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception Handler for Online Slow Log Ring Buffer
 */
@InterfaceAudience.Private
class DisruptorExceptionHandler implements ExceptionHandler<RingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(DisruptorExceptionHandler.class);

  @Override
  public void handleEventException(Throwable e, long sequence, RingBufferEnvelope event) {
    LOG.error("Sequence={}, event={}", sequence, event, e);
  }

  @Override
  public void handleOnStartException(Throwable e) {
    LOG.error("Disruptor onStartException: ", e);
  }

  @Override
  public void handleOnShutdownException(Throwable e) {
    LOG.error("Disruptor onShutdownException: ", e);
  }

}
