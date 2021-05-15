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
package org.apache.hadoop.hbase.master.http.jersey;

import java.util.function.Supplier;
import javax.inject.Singleton;
import org.apache.hadoop.hbase.http.jersey.SupplierFactoryAdapter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Feature;
import org.apache.hbase.thirdparty.javax.ws.rs.core.FeatureContext;
import org.apache.hbase.thirdparty.org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.apache.hbase.thirdparty.org.glassfish.hk2.utilities.binding.ServiceBindingBuilder;

/**
 * Implements a Singleton binding to the provided instance of {@link HMaster} for both
 * {@link HMaster} and {@link MasterServices} injections.
 */
@InterfaceAudience.Private
public class MasterFeature implements Feature {

  private final Supplier<HMaster> supplier;

  public MasterFeature(HMaster master) {
    this.supplier = () -> master;
  }

  @Override
  public boolean configure(FeatureContext context) {
    context.register(new Binder());
    return true;
  }

  /**
   * Register this feature's provided functionality and defines their lifetime scopes.
   */
  private class Binder extends AbstractBinder {

    @Override
    protected void configure() {
      bindFactory(supplier)
        .to(HMaster.class)
        .in(Singleton.class);
      bindFactory(supplier)
        .to(MasterServices.class)
        .in(Singleton.class);
    }

    /**
     * Helper method for smoothing over use of {@link SupplierFactoryAdapter}. Inspired by internal
     * implementation details of jersey itself.
     */
    private <T> ServiceBindingBuilder<T> bindFactory(Supplier<T> supplier) {
      return bindFactory(new SupplierFactoryAdapter<>(supplier));
    }
  }
}
