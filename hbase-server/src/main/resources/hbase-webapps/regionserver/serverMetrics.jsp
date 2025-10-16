<%--
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
--%>
<%@ page contentType="text/html;charset=UTF-8" %>

<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item"><a class="nav-link active" href="#tab_baseStats" data-bs-toggle="tab" role="tab">Base Stats</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_memoryStats" data-bs-toggle="tab" role="tab">Memory</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_requestStats" data-bs-toggle="tab" role="tab">Requests</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_walStats" data-bs-toggle="tab" role="tab">WALs</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_storeStats" data-bs-toggle="tab" role="tab">Storefiles</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_queueStats" data-bs-toggle="tab" role="tab">Queues</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_byteBuffAllocatorStats" data-bs-toggle="tab" role="tab">ByteBuffAllocator Stats</a></li>
  </ul>
  <div class="tab-content">
    <div class="tab-pane active" id="tab_baseStats" role="tabpanel">
      <jsp:include page="serverMetricsBaseStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_memoryStats" role="tabpanel">
      <jsp:include page="serverMetricsMemoryStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_requestStats" role="tabpanel">
      <jsp:include page="serverMetricsRequestStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_walStats" role="tabpanel">
      <jsp:include page="serverMetricsWalStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_storeStats" role="tabpanel">
      <jsp:include page="serverMetricsStoreStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_queueStats" role="tabpanel">
      <jsp:include page="serverMetricsQueueStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_byteBuffAllocatorStats" role="tabpanel">
      <jsp:include page="serverMetricsByteBuffAllocatorStats.jsp"/>
    </div>
  </div>
</div>
