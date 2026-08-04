/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <string.h>

#include <aerospike/as_config.h>

#include "test.h"

//---------------------------------
// Static Functions
//---------------------------------

// Fills buf (size bytes, including the null terminator) with 'a' characters
// followed by a null terminator, producing a string of length (size - 1).
static void
fill_string(char* buf, size_t size)
{
	memset(buf, 'a', size - 1);
	buf[size - 1] = 0;
}

static void
seed_config(as_config* config)
{
	as_config_init(config);
	strcpy(config->user, "existing_user");
	strcpy(config->password, "existing_pass");
}

static void
assert_config_unchanged(
	atf_test_result* __result__, as_config* config, const char* user_snapshot,
	const char* password_snapshot)
{
	if (memcmp(config->user, user_snapshot, sizeof(config->user)) != 0) {
		return atf_assert_bytes_eq(__result__, "config.user", (uint8_t*)config->user,
			sizeof(config->user), (uint8_t*)user_snapshot, sizeof(config->user), __FILE__, __LINE__);
	}

	if (memcmp(config->password, password_snapshot, sizeof(config->password)) != 0) {
		return atf_assert_bytes_eq(__result__, "config.password", (uint8_t*)config->password,
			sizeof(config->password), (uint8_t*)password_snapshot, sizeof(config->password),
			__FILE__, __LINE__);
	}
}

//---------------------------------
// Test Cases
//---------------------------------

TEST(config_set_user_valid, "as_config_set_user() accepts valid length user/password")
{
	as_config config;
	as_config_init(&config);

	assert_true(as_config_set_user(&config, "charlie", "mypassword"));
	assert_string_eq(config.user, "charlie");
	assert_string_eq(config.password, "mypassword");
}

TEST(config_set_user_null_password, "as_config_set_user() accepts a NULL password")
{
	as_config config;
	as_config_init(&config);

	assert_true(as_config_set_user(&config, "charlie", NULL));
	assert_string_eq(config.user, "charlie");
	assert_int_eq(config.password[0], 0);
}

TEST(config_set_user_empty_username, "as_config_set_user() rejects empty/NULL username without touching config")
{
	as_config config;
	char user_snapshot[AS_USER_SIZE];
	char password_snapshot[AS_PASSWORD_SIZE];

	seed_config(&config);
	memcpy(user_snapshot, config.user, sizeof(user_snapshot));
	memcpy(password_snapshot, config.password, sizeof(password_snapshot));

	assert_false(as_config_set_user(&config, "", "somepassword"));
	assert_config_unchanged(__result__, &config, user_snapshot, password_snapshot);

	assert_false(as_config_set_user(&config, NULL, "somepassword"));
	assert_config_unchanged(__result__, &config, user_snapshot, password_snapshot);
}

TEST(config_set_user_oversized_user, "as_config_set_user() rejects an oversized username without any partial write")
{
	as_config config;
	char user_snapshot[AS_USER_SIZE];
	char password_snapshot[AS_PASSWORD_SIZE];
	char oversized_user[AS_USER_SIZE + 1];

	seed_config(&config);
	memcpy(user_snapshot, config.user, sizeof(user_snapshot));
	memcpy(password_snapshot, config.password, sizeof(password_snapshot));

	// Length == AS_USER_SIZE (the null terminator would not fit).
	fill_string(oversized_user, sizeof(oversized_user));

	assert_false(as_config_set_user(&config, oversized_user, "shortpass"));
	assert_config_unchanged(__result__, &config, user_snapshot, password_snapshot);
}

TEST(config_set_user_oversized_password, "as_config_set_user() rejects an oversized password without any partial write")
{
	as_config config;
	char user_snapshot[AS_USER_SIZE];
	char password_snapshot[AS_PASSWORD_SIZE];
	char oversized_password[AS_PASSWORD_SIZE + 1];

	seed_config(&config);
	memcpy(user_snapshot, config.user, sizeof(user_snapshot));
	memcpy(password_snapshot, config.password, sizeof(password_snapshot));

	// Length == AS_PASSWORD_SIZE (the null terminator would not fit).
	fill_string(oversized_password, sizeof(oversized_password));

	assert_false(as_config_set_user(&config, "shortuser", oversized_password));
	assert_config_unchanged(__result__, &config, user_snapshot, password_snapshot);
}

TEST(config_set_user_both_oversized, "as_config_set_user() rejects oversized user and password together without any partial write")
{
	as_config config;
	char user_snapshot[AS_USER_SIZE];
	char password_snapshot[AS_PASSWORD_SIZE];
	char oversized_user[AS_USER_SIZE + 1];
	char oversized_password[AS_PASSWORD_SIZE + 1];

	seed_config(&config);
	memcpy(user_snapshot, config.user, sizeof(user_snapshot));
	memcpy(password_snapshot, config.password, sizeof(password_snapshot));

	fill_string(oversized_user, sizeof(oversized_user));
	fill_string(oversized_password, sizeof(oversized_password));

	assert_false(as_config_set_user(&config, oversized_user, oversized_password));
	assert_config_unchanged(__result__, &config, user_snapshot, password_snapshot);
}

SUITE(config_basics, "aerospike config tests")
{
	suite_add(config_set_user_valid);
	suite_add(config_set_user_null_password);
	suite_add(config_set_user_empty_username);
	suite_add(config_set_user_oversized_user);
	suite_add(config_set_user_oversized_password);
	suite_add(config_set_user_both_oversized);
}