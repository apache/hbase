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
package org.apache.hadoop.hbase.regionserver;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;

/**
 * This callable is used to do the real split WAL task. It is called by
 * {@link org.apache.hadoop.hbase.master.procedure.SplitWALRemoteProcedure} from master and executed
 * by executor service which is in charge of executing the events of EventType.RS_LOG_REPLAY
 *
 * When execute this callable, it will call SplitLogWorker.splitLog() to split the WAL.
 * If the return value is SplitLogWorker.TaskExecutor.Status.DONE, it means the task is successful
 * and it will return null to end the call. Otherwise it will throw an exception and let
 * {@link org.apache.hadoop.hbase.master.procedure.SplitWALRemoteProcedure} to handle this problem.
 *
 * This class is to replace the zk-based WAL splitting related code, {@link SplitLogWorker},
 * {@link org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination} and
 * {@link org.apache.hadoop.hbase.coordination.ZkSplitLogWorkerCoordination} can be removed after
 * we switch to procedure-based WAL splitting.
 */
@InterfaceAudience.Private
public class SplitWALCallable implements RSProcedureCallable {
  private static final Logger LOG = LoggerFactory.getLogger(SplitWALCallable.class);

  private String walPath;
  private Exception initError;
  private HRegionServer rs;
  private final KeyLocker<String> splitWALLocks = new KeyLocker<>();
  private volatile Lock splitWALLock = null;


  @Override
  public void init(byte[] parameter, HRegionServer rs) {
    try {
      this.rs = rs;
      MasterProcedureProtos.SplitWALParameter param =
          MasterProcedureProtos.SplitWALParameter.parseFrom(parameter);
      this.walPath = param.getWalPath();
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Parse proto buffer of split WAL request failed ", e);
      initError = e;
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_LOG_REPLAY;
  }

  public static class PreemptedWALSplitException extends HBaseIOException {
    PreemptedWALSplitException(String wal) {
      super(wal);
    }
  }

  public static class ResignedWALSplitException extends HBaseIOException {
    ResignedWALSplitException(String wal) {
      super(wal);
    }
  }

  public static class ErrorWALSplitException extends HBaseIOException {
    ErrorWALSplitException(String wal) {
      super(wal);
    }
  }

  @Override
  public Void call() throws Exception {
    if (initError != null) {
      throw initError;
    }
    //grab a lock
    splitWALLock = splitWALLocks.acquireLock(walPath);
    try {
      switch (SplitLogWorker.splitLog(walPath, null, rs.getConfiguration(), rs, rs, rs.getWalFactory())) {
        case DONE:
          break;
        case PREEMPTED:
          throw new PreemptedWALSplitException(this.walPath);
        case RESIGNED:
          throw new ResignedWALSplitException(this.walPath);
        default:
          throw new ErrorWALSplitException(this.walPath);
      }
    } finally {
      splitWALLock.unlock();
    }
    return null;
  }

  public String getWalPath() {
    return this.walPath;
  }
}
