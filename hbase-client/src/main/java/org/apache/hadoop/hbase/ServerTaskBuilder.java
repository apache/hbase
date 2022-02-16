/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/** Builder for information about active monitored server tasks */
@InterfaceAudience.Private
public final class ServerTaskBuilder {

  public static ServerTaskBuilder newBuilder() {
    return new ServerTaskBuilder();
  }

  private String description = "";
  private String status = "";
  private ServerTask.State state = ServerTask.State.RUNNING;
  private long startTime;
  private long completionTime;

  private ServerTaskBuilder() { }

  private static final class ServerTaskImpl implements ServerTask {

    private final String description;
    private final String status;
    private final ServerTask.State state;
    private final long startTime;
    private final long completionTime;

    private ServerTaskImpl(final String description, final String status,
        final ServerTask.State state, final long startTime, final long completionTime) {
      this.description = description;
      this.status = status;
      this.state = state;
      this.startTime = startTime;
      this.completionTime = completionTime;
    }

    @Override
    public String getDescription() {
      return description;
    }

    @Override
    public String getStatus() {
      return status;
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public long getCompletionTime() {
      return completionTime;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(512);
      sb.append(getDescription());
      sb.append(": status=");
      sb.append(getStatus());
      sb.append(", state=");
      sb.append(getState());
      sb.append(", startTime=");
      sb.append(getStartTime());
      sb.append(", completionTime=");
      sb.append(getCompletionTime());
      return sb.toString();
    }

  }

  public ServerTaskBuilder setDescription(final String description) {
    this.description = description;
    return this;
  }

  public ServerTaskBuilder setStatus(final String status) {
    this.status = status;
    return this;
  }

  public ServerTaskBuilder setState(final ServerTask.State state) {
    this.state = state;
    return this;
  }

  public ServerTaskBuilder setStartTime(final long startTime) {
    this.startTime = startTime;
    return this;
  }

  public ServerTaskBuilder setCompletionTime(final long completionTime) {
    this.completionTime = completionTime;
    return this;
  }

  public ServerTask build() {
    return new ServerTaskImpl(description, status, state, startTime, completionTime);
  }

}
