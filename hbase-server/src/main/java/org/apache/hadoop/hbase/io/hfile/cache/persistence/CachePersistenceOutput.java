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
package org.apache.hadoop.hbase.io.hfile.cache.persistence;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Represents an in-progress cache persistence write.
 * <p>
 * A persistence write is not visible as committed state until {@link #commit()} completes
 * successfully. If the write is abandoned or an error occurs before commit, {@link #abort()}
 * discards the incomplete state.
 * </p>
 * <p>
 * {@link #close()} must abort an uncommitted write. This makes the handle safe to use with
 * try-with-resources when a cache component throws while saving its state.
 * </p>
 * <p>
 * The output stream returned by {@link #getOutputStream()} is owned by this handle. Cache
 * components may write to the stream but must not close it directly.
 * </p>
 */
@InterfaceAudience.Private
public interface CachePersistenceOutput extends AutoCloseable {

  /**
   * Returns the output stream to which the cache component should write its persisted state.
   * <p>
   * The returned stream must not be closed by the cache component.
   * </p>
   * @return output stream for persisted component state
   */
  OutputStream getOutputStream();

  /**
   * Commits the completed persistence write.
   * <p>
   * After this method returns successfully, the newly written state becomes the state returned by
   * subsequent persistence reads for the associated storage key.
   * </p>
   * @throws IOException if the state cannot be committed
   */
  void commit() throws IOException;

  /**
   * Aborts the persistence write and discards any incomplete state.
   * <p>
   * Calling this method after a successful commit has no effect.
   * </p>
   * @throws IOException if temporary state cannot be discarded
   */
  void abort() throws IOException;

  /**
   * Closes this persistence output.
   * <p>
   * If the write has not been committed, closing the handle aborts it. Closing the handle must
   * never implicitly commit an unfinished write.
   * </p>
   * @throws IOException if the output cannot be closed or temporary state cannot be discarded
   */
  @Override
  void close() throws IOException;
}
