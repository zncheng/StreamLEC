/*
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   *************************************************************************
   NOTE to contributors. This file comprises the principal public contract
   for StreamLEC API users. Any change to this file
   supplied in a stable release SHOULD not break existing applications.
   In practice this means that the value of constants must not change, and
   that old values may not be reused for new constants.
   *************************************************************************
*/

#ifndef __CS_H_INCLUDED__
#define __CS_H_INCLUDED__

/*  Version macros for compile-time API version detection                     */
#define CS_VERSION_MAJOR 1
#define CS_VERSION_MINOR 0
#define CS_VERSION_PATCH 0

#define CS_MAKE_VERSION(major, minor, patch) \
    ((major) * 10000 + (minor) * 100 + (patch))
#define CS_VERSION \
    CS_MAKE_VERSION(CS_VERSION_MAJOR, CS_VERSION_MINOR, CS_VERSION_PATCH)

#include <stdint.h>

/*******************************************************************
 *
 * Utilities
 *
 ******************************************************************/

#include "../src/config.hpp"
#include "../src/util.hpp"

/*******************************************************************
 *
 * Threads
 *
 ******************************************************************/

#include "../src/thread/worker.hpp"
#include "../src/thread/up_thread_trace.hpp"
#include "../src/thread/up_thread_trace_afs.hpp"
#include "../src/thread/router_rr.hpp"
#include "../src/thread/down_thread_net.hpp"
#include "../src/thread/compute_thread.hpp"
#include "../src/thread/main_thread.hpp"

#include "../src/raw_item.hpp"
#include "../src/raw_item_d.hpp"
#include "../src/util.hpp"
#include "../src/coding/coding_item.hpp"
#include "../src/coding/matrix_operation.hpp"
#include "../src/coding/data_type.hpp"

#endif

