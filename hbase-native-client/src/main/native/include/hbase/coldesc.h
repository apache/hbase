/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef LIBHBASE_COLDESC_H_
#define LIBHBASE_COLDESC_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Creates a column family descriptor.
 * @returns a handle to an hb_columndesc object or NULL if unsuccessful.
 */
HBASE_API int32_t
hb_coldesc_create(
    const byte_t *family,    /* [in] Null terminated column family name */
    const size_t family_len,
    hb_columndesc *col_desc);

/**
 * Release resources held by column family descriptor.
 */
HBASE_API int32_t
hb_coldesc_destroy(
    hb_columndesc col_desc); /* [in] hb_columndesc handle */

/**
 * Sets the maximum number of cell versions to be retained for the column
 * family. Defaults to 3.
 */
HBASE_API int32_t
hb_coldesc_set_maxversions(
    hb_columndesc col_desc, /* [in] hb_columndesc handle */
    int32_t max_versions);  /* [in] maximum number of versions */

/**
 * Sets the minimum number of cell versions to be retained for the column
 * family. Defaults to 0.
 */
HBASE_API int32_t
hb_coldesc_set_minversions(
    hb_columndesc col_desc, /* [in] hb_columndesc handle */
    int32_t min_versions);  /* [in] minimum number of versions */

/**
 * Sets the time-to-live of cell contents, in seconds. Defaults is forever.
 */
HBASE_API int32_t
hb_coldesc_set_ttl(
    hb_columndesc col_desc, /* [in] hb_columndesc handle */
    int32_t ttl); /* [in] time-to-live of cell contents, in seconds */

/**
 * Sets if all values are to keep in the HRegionServer cache. Defaults to
 * 0 (false).
 */
HBASE_API int32_t
hb_coldesc_set_inmemory(
    hb_columndesc col_desc, /* [in] hb_columndesc handle */
    int32_t inmemory);      /* [in] 0 for false, true otherwise */

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // LIBHBASE_COLDESC_H_
