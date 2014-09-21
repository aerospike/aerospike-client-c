/*
 * Copyright 2008-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

#include <stdio.h>

// Comment out this line to turn OFF debugging.
// #define DEBUG

#ifdef DEBUG
// With debugging defined, we'll have these mechanisms for printing
#define INFO(fmt, args...)  __log_append(stderr,"", fmt, ## args);
#define ERROR(fmt, args...) __log_append(stderr,"    ", fmt, ## args);
#define LOG(fmt, args...)   __log_append(stderr,"    ", fmt, ## args);
#else
#define INFO(fmt, args...) 
#define ERROR(fmt, args...) 
#define LOG(fmt, args...)  
#endif


// Forward declare the log call.
extern void __log_append(FILE * f, const char * prefix, const char * fmt, ... );

