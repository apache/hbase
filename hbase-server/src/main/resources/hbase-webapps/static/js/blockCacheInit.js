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

/*
 * This script displays and paginates the initially hidden table rows of Hit Ratios for time periods.
 * See: HBASE-29276 Compute and display hit ratio by configurable, granular time periods
 */

// Wait for document to be fully loaded
document.addEventListener('DOMContentLoaded', function() {

  // Count actual items in the DOM
  const itemRows = document.querySelectorAll('tr.item-row');

  // Pagination state
  let currentPage = 1;
  const pageSize = 10;
  const totalItems = itemRows.length;
  const totalPages = Math.ceil(totalItems / pageSize);

  // Create page buttons
  const pageButtonsContainer = document.getElementById('page-buttons');
  if (pageButtonsContainer) {
    for (let i = 1; i <= totalPages; i++) {
      const button = document.createElement('button');
      button.className = 'page-number';
      button.textContent = i;
      button.onclick = function() { goToPage(i); };
      pageButtonsContainer.appendChild(button);
    }
  }
  function displayItems() {
    // Hide all item rows
    itemRows.forEach(row => {
      row.style.display = 'none';
    });

    // Calculate indexes
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, totalItems);

    // Show rows for current page
    let displayedCount = 0;
    for (let i = startIndex; i < endIndex; i++) {
      const row = document.getElementById('row-' + i);
      if (row) {
        row.style.display = 'table-row';
        displayedCount++;
      }
    }

    // Update pagination UI
    document.querySelectorAll('.page-number').forEach(btn => {
      if (parseInt(btn.textContent) === currentPage) {
        btn.classList.add('active');
      } else {
        btn.classList.remove('active');
      }
    });

    const prevBtn = document.getElementById('prev-page');
    const nextBtn = document.getElementById('next-page');

    if (prevBtn) prevBtn.disabled = currentPage === 1;
    if (nextBtn) nextBtn.disabled = currentPage === totalPages;

    // Update page info
    const pageInfo = document.getElementById('page-info');
    if (pageInfo) {
      pageInfo.textContent = `Showing ${startIndex + 1} to ${endIndex} of ${totalItems} items`;
    }
  }

  function goToPage(page) {
    if (page >= 1 && page <= totalPages) {
      currentPage = page;
      displayItems();
    }
  }

  window.nextPage = function() {
    goToPage(currentPage + 1);
  };

  window.prevPage = function() {
    goToPage(currentPage - 1);
  };

  window.goToPage = goToPage;

  // Check URL for initial page
  const urlParams = new URLSearchParams(window.location.search);
  const pageParam = urlParams.get('page');
  if (pageParam) {
    const parsedPage = parseInt(pageParam);
    if (!isNaN(parsedPage) && parsedPage >= 1) {
      currentPage = parsedPage;
    }
  }

  // Initial display
  displayItems();
});
