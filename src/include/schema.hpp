#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"

namespace intellekt::duckpst::schema {
using namespace duckdb;

/* Virtual columns */
// TODO: this is an extern and we want constexpr, so copy it for now
static constexpr column_t DUCKDB_VIRTUAL_COLUMN_START = UINT64_C(9223372036854775808);
static constexpr auto PST_PARTITION_INDEX = DUCKDB_VIRTUAL_COLUMN_START;
static constexpr auto PST_PARTITION_INDEX_TYPE = LogicalType::UBIGINT;

static constexpr auto PST_ITEM_NODE_ID = DUCKDB_VIRTUAL_COLUMN_START + 1;
static constexpr auto PST_ITEM_NODE_ID_TYPE = LogicalType::UINTEGER;

/* Enum schemas */
inline LogicalType RecipientTypeSchema() {
	Vector values(LogicalType::VARCHAR, 3);
	auto data = FlatVector::GetData<string_t>(values);
	data[0] = StringVector::AddString(values, "TO");
	data[1] = StringVector::AddString(values, "CC");
	data[2] = StringVector::AddString(values, "BCC");
	return LogicalType::ENUM(values, 3);
}

inline LogicalType ImportanceSchema() {
	Vector values(LogicalType::VARCHAR, 3);
	auto data = FlatVector::GetData<string_t>(values);
	data[0] = StringVector::AddString(values, "LOW");
	data[1] = StringVector::AddString(values, "NORMAL");
	data[2] = StringVector::AddString(values, "HIGH");
	return LogicalType::ENUM(values, 3);
}

inline LogicalType SensitivitySchema() {
	Vector values(LogicalType::VARCHAR, 4);
	auto data = FlatVector::GetData<string_t>(values);
	data[0] = StringVector::AddString(values, "NONE");
	data[1] = StringVector::AddString(values, "PERSONAL");
	data[2] = StringVector::AddString(values, "PRIVATE");
	data[3] = StringVector::AddString(values, "CONFIDENTIAL");
	return LogicalType::ENUM(values, 4);
}

inline LogicalType AttachMethodSchema() {
	Vector values(LogicalType::VARCHAR, 7);
	auto data = FlatVector::GetData<string_t>(values);
	data[0] = StringVector::AddString(values, "NO_ATTACHMENT");
	data[1] = StringVector::AddString(values, "BY_VALUE");
	data[2] = StringVector::AddString(values, "BY_REFERENCE");
	data[3] = StringVector::AddString(values, "BY_REF_RESOLVE");
	data[4] = StringVector::AddString(values, "BY_REF_ONLY");
	data[5] = StringVector::AddString(values, "EMBEDDED_MESSAGE");
	data[6] = StringVector::AddString(values, "OLE");
	return LogicalType::ENUM(values, 7);
}

static const auto RECIPIENT_TYPE_ENUM = RecipientTypeSchema();
static const auto IMPORTANCE_ENUM = ImportanceSchema();
static const auto SENSITIVITY_ENUM = SensitivitySchema();
static const auto ATTACH_METHOD_ENUM = AttachMethodSchema();

// We'll generate our table function output schemas using x-macros so the serialization code
// doesn't have to bind against a position ordinal and we can move columns around
#define SCHEMA_CHILD(name, type)     {#name, type},
#define SCHEMA_CHILD_NAME(name, ...) name,

/* Per-file PST attributes */

#define PST_CHILDREN(LT)                                                                                               \
	LT(pst_path, LogicalType::VARCHAR)                                                                                 \
	LT(pst_name, LogicalType::VARCHAR)                                                                                 \
	LT(record_key, LogicalType::BLOB)                                                                                  \
	LT(node_id, LogicalType::UINTEGER)

enum class PSTProjection { PST_CHILDREN(SCHEMA_CHILD_NAME) };
static const auto PST_SCHEMA = LogicalType::STRUCT({PST_CHILDREN(SCHEMA_CHILD)});

/* Common MAPI attributes schema */

#define COMMON_CHILDREN(LT)                                                                                            \
	/* TODO: these are computed properties (see spec: 2.4.3.2 Mapping between EntryID and NID) */                      \
	/* LT(entry_id, LogicalType::BLOB) */                                                                              \
	/* LT(parent_entry_id, LogicalType::BLOB) */                                                                       \
	LT(display_name, LogicalType::VARCHAR)                                                                             \
	LT(comment, LogicalType::VARCHAR)                                                                                  \
	LT(creation_time, LogicalType::TIMESTAMP_S)                                                                        \
	LT(last_modified, LogicalType::TIMESTAMP_S)

enum class CommonProjection { COMMON_CHILDREN(SCHEMA_CHILD_NAME) NUM_FIELDS };

static const auto COMMON_SCHEMA = LogicalType::STRUCT({COMMON_CHILDREN(SCHEMA_CHILD)});

/* Recipient struct schema */

#define RECIPIENT_CHILDREN(LT)                                                                                         \
	LT(account_name, LogicalType::VARCHAR)                                                                             \
	LT(email_address, LogicalType::VARCHAR)                                                                            \
	LT(address_type, LogicalType::VARCHAR)                                                                             \
	LT(recipient_type, RECIPIENT_TYPE_ENUM)                                                                            \
	LT(recipient_type_raw, LogicalType::INTEGER)

enum class RecipientProjection { COMMON_CHILDREN(SCHEMA_CHILD_NAME) RECIPIENT_CHILDREN(SCHEMA_CHILD_NAME) };

static const auto RECIPIENT_SCHEMA =
    LogicalType::STRUCT({COMMON_CHILDREN(SCHEMA_CHILD) RECIPIENT_CHILDREN(SCHEMA_CHILD)});

/* Attachment struct schema */

#define ATTACHMENT_CHILDREN(LT)                                                                                        \
	LT(attach_content_id, LogicalType::VARCHAR)                                                                        \
	LT(attach_method, ATTACH_METHOD_ENUM)                                                                              \
	LT(filename, LogicalType::VARCHAR)                                                                                 \
	LT(mime_type, LogicalType::VARCHAR)                                                                                \
	LT(size, LogicalType::UBIGINT)                                                                                     \
	LT(is_message, LogicalType::BOOLEAN)                                                                               \
	LT(bytes, LogicalType::BLOB)

enum class AttachmentProjection { ATTACHMENT_CHILDREN(SCHEMA_CHILD_NAME) };

static const auto ATTACHMENT_SCHEMA = LogicalType::STRUCT({ATTACHMENT_CHILDREN(SCHEMA_CHILD)});

/* Common fields in message and folder */
enum class PSTCommonChildren {

	PST_CHILDREN(SCHEMA_CHILD_NAME) COMMON_CHILDREN(SCHEMA_CHILD_NAME) NUM_FIELDS
};

/* Message schema */

#define MESSAGE_CHILDREN(LT)                                                                                           \
	LT(subject, LogicalType::VARCHAR)                                                                                  \
	LT(sender_name, LogicalType::VARCHAR)                                                                              \
	LT(sender_email_address, LogicalType::VARCHAR)                                                                     \
	LT(message_delivery_time, LogicalType::TIMESTAMP_S)                                                                \
	LT(message_class, LogicalType::VARCHAR)                                                                            \
	LT(importance, IMPORTANCE_ENUM)                                                                                    \
	LT(sensitivity, SENSITIVITY_ENUM)                                                                                  \
	LT(message_flags, LogicalType::INTEGER)                                                                            \
	LT(message_size, LogicalType::UBIGINT)                                                                             \
	LT(has_attachments, LogicalType::BOOLEAN)                                                                          \
	LT(attachment_count, LogicalType::UINTEGER)                                                                        \
	LT(body, LogicalType::VARCHAR)                                                                                     \
	LT(body_html, LogicalType::VARCHAR)                                                                                \
	LT(internet_message_id, LogicalType::VARCHAR)                                                                      \
	LT(conversation_topic, LogicalType::VARCHAR)                                                                       \
	LT(recipients, LogicalType::LIST(RECIPIENT_SCHEMA))                                                                \
	LT(attachments, LogicalType::LIST(ATTACHMENT_SCHEMA))

enum class MessageProjection {
	PST_CHILDREN(SCHEMA_CHILD_NAME) COMMON_CHILDREN(SCHEMA_CHILD_NAME) MESSAGE_CHILDREN(SCHEMA_CHILD_NAME)
};

static const auto MESSAGE_SCHEMA =
    LogicalType::STRUCT({PST_CHILDREN(SCHEMA_CHILD) COMMON_CHILDREN(SCHEMA_CHILD) MESSAGE_CHILDREN(SCHEMA_CHILD)});

/* Folder schema */

#define FOLDER_CHILDREN(LT)                                                                                            \
	LT(display_name, LogicalType::VARCHAR)                                                                             \
	LT(parent_node_id, LogicalType::UINTEGER)                                                                          \
	LT(subfolder_count, LogicalType::UINTEGER)                                                                         \
	LT(message_count, LogicalType::BIGINT)                                                                             \
	LT(unread_message_count, LogicalType::BIGINT)

enum class FolderProjection { PST_CHILDREN(SCHEMA_CHILD_NAME) FOLDER_CHILDREN(SCHEMA_CHILD_NAME) };

static const auto FOLDER_SCHEMA = LogicalType::STRUCT({PST_CHILDREN(SCHEMA_CHILD) FOLDER_CHILDREN(SCHEMA_CHILD)});
} // namespace intellekt::duckpst::schema