#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest

import python.test.test_dual_active_cluster_startup as test_dual_active_cluster_startup
import python.test.test_create_drop_behavior as test_create_drop_behavior
import python.test.test_put_get_delete_behavior as test_put_get_delete_behavior
import python.test.test_read_only_flag_flipping as test_read_only_flag_flipping
import python.test.test_cannot_promote_second_active_cluster as test_cannot_promote_second_active_cluster
import python.test.test_bulkloaded_data_and_region_splits as test_bulkloaded_data_and_region_splits


@pytest.mark.flaky(reruns=2, reruns_delay=2)
class TestReadReplica:
    def test_dual_active_cluster_startup(self):
        test_dual_active_cluster_startup.run_test(clean_up_containers=True)

    def test_create_drop_behavior(self):
        test_create_drop_behavior.run_test(drop_existing_tables=True, new_containers=True)

    def test_put_get_delete_behavior(self):
        test_put_get_delete_behavior.run_test(drop_existing_tables=True, new_containers=True)

    def test_read_only_flag_flipping(self):
        test_read_only_flag_flipping.run_test(new_containers=True)

    def test_cannot_promote_second_active_cluster(self):
        test_cannot_promote_second_active_cluster.run_test(new_containers=True)

    def test_bulkloaded_data_and_region_splits(self):
        test_bulkloaded_data_and_region_splits.run_test(new_containers=True)
