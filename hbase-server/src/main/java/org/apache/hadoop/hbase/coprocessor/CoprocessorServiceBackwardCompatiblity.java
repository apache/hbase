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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import com.google.protobuf.Service;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.Collections;

/**
 * Classes to help maintain backward compatibility with now deprecated {@link CoprocessorService}
 * and {@link SingletonCoprocessorService}.
 * From 2.0 onwards, implementors of coprocessor service should also implement the relevant
 * coprocessor class (For eg {@link MasterCoprocessor} for coprocessor service in master), and
 * override get*Service() method to return the {@link com.google.protobuf.Service} object.
 * To maintain backward compatibility with 1.0 implementation, we'll wrap implementation of
 * CoprocessorService/SingletonCoprocessorService in the new
 * {Master, Region, RegionServer}Coprocessor class.
 * Since there is no backward compatibility guarantee for Observers, we leave get*Observer() to
 * default which returns null.
 * This approach to maintain backward compatibility seems cleaner and more explicit.
 */
@InterfaceAudience.Private
@Deprecated
public class CoprocessorServiceBackwardCompatiblity {

  static public class MasterCoprocessorService implements MasterCoprocessor {

    CoprocessorService service;

    public MasterCoprocessorService(CoprocessorService service) {
      this.service = service;
    }

    @Override
    public Iterable<Service> getServices() {
      return Collections.singleton(service.getService());
    }
  }

  static public class RegionCoprocessorService implements RegionCoprocessor {

    CoprocessorService service;

    public RegionCoprocessorService(CoprocessorService service) {
      this.service = service;
    }

    @Override
    public Iterable<Service> getServices() {
      return Collections.singleton(service.getService());
    }
  }

  static public class RegionServerCoprocessorService implements RegionServerCoprocessor {

    SingletonCoprocessorService service;

    public RegionServerCoprocessorService(SingletonCoprocessorService service) {
      this.service = service;
    }

    @Override
    public Iterable<Service> getServices() {
      return Collections.singleton(service.getService());
    }
  }
}

