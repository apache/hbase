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

import java.io.IOException;
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * If set of MapFile.Readers in Store change, implementors are notified.
 */
@InterfaceAudience.Private
public interface ChangedReadersObserver {

  /** Returns the read point of the current scan */
  long getReadPoint();

  /**
   * Notify observers. <br/>
   * NOTE:Before we invoke this method,{@link HStoreFile#increaseRefCount} is invoked for every
   * {@link HStoreFile} in 'sfs' input parameter to prevent {@link HStoreFile} is archived after a
   * concurrent compaction, and after this method is invoked,{@link HStoreFile#decreaseRefCount} is
   * invoked.So if you open the {@link StoreFileReader} or {@link StoreFileScanner} asynchronously
   * in this method,you may need to invoke {@link HStoreFile#increaseRefCount} or
   * {@link HStoreFile#decreaseRefCount} by yourself to prevent the {@link HStoreFile}s be archived.
   * @param sfs              The new files
   * @param memStoreScanners scanner of current memstore
   * @throws IOException e
   */
  void updateReaders(List<HStoreFile> sfs, List<KeyValueScanner> memStoreScanners)
    throws IOException;
}
