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
package org.apache.hadoop.hbase.wal;

import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;

/*
 * This is distributed FS oriented implementation for WALIdentity
 */
@InterfaceAudience.Private
public class FSWALIdentity implements WALIdentity{
  private String name;
  private Path path;

  public FSWALIdentity(String name) {
    this.path = new Path(name);
    this.name = path.getName();
  }

  public FSWALIdentity(Path path) {
    this.path = path;
    if (path !=null) {
      this.name = path.getName();
    }
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * @return {@link Path} object of the name encapsulated in WALIdentity
   */
  public Path getPath() {
    return path;
  }

  @Override
  public int compareTo(WALIdentity o) {
    FSWALIdentity that = (FSWALIdentity)o;
    return this.path.compareTo(that.getPath());
  }

  @Override
  public String toString() {
    return this.path.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FSWALIdentity)) {
      return false;
    }
    FSWALIdentity that = (FSWALIdentity) obj;
    return this.path.equals(that.getPath());
  }
  @Override
  public int hashCode() {
    return this.path.hashCode();
  }
}
