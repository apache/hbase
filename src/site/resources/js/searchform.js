/*
 * Copyright The Apache Software Foundation
 *
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

// This script appends our Google Site search form to the navbar.
// Unfortunately our current Maven site skin does support this.

window.onload = function() {
  const div = document.getElementById('top-navbar-collapse-1');

  const form = document.createElement("form");
  form.setAttribute('id', 'search-form');
  form.setAttribute('action', 'https://www.google.com/search');
  form.setAttribute('method', 'get');
  form.setAttribute('class', 'form-inline ml-lg-3');

  const siteSearchInput = document.createElement('input');
  siteSearchInput.setAttribute('value', 'hbase.apache.org');
  siteSearchInput.setAttribute('name', 'sitesearch');
  siteSearchInput.setAttribute('type', 'hidden');
  form.appendChild(siteSearchInput);

  const queryInput = document.createElement('input');
  queryInput.setAttribute('name', 'q');
  queryInput.setAttribute('id', 'query');
  queryInput.setAttribute('type', 'text');
  queryInput.setAttribute('placeholder', 'Search with Google...');
  queryInput.setAttribute('class', 'form-control');
  form.appendChild(queryInput);

  div.appendChild(form);
};
