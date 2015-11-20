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
 *
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The following code block define API as the tag for exported
 * functions. The library should be compiled with symbols visibility
 * set to hidden by default and only the exported functions should be
 * tagged as HBASE_API.
 *
 * When building the library on Windows, compile with compiler flag
 * "-D_LIBHBASE_IMPLEMENTATION_", whereas when linking application with
 * this library, this compiler flag should not be used.
 */
#if defined _WIN32 || defined __CYGWIN__
#ifdef _LIBHBASE_IMPLEMENTATION_
#define API __declspec(dllexport)
#else
#ifdef _LIBHBASE_TEST_
#define HBASE_API
#else
#define HBASE_API __declspec(dllimport)
#endif
#endif
#else
#if __GNUC__ >= 4
#define HBASE_API __attribute__((visibility("default")))
#else
#define HBASE_API
#endif
#endif

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
