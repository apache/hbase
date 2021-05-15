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
package org.apache.hadoop.hbase.http.jersey;

import java.util.function.Supplier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.org.glassfish.hk2.api.Factory;

/**
 * Use a {@link Supplier} of type {@code T} as a {@link Factory} that provides instances of
 * {@code T}. Modeled after Jersey's internal implementation.
 */
@InterfaceAudience.Private
public class SupplierFactoryAdapter<T> implements Factory<T> {

  private final Supplier<T> supplier;

  public SupplierFactoryAdapter(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  @Override public T provide() {
    return supplier.get();
  }

  @Override public void dispose(T instance) { }
}
