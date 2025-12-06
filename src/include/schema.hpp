#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"

namespace intellekt::duckpst::schema {
using namespace duckdb;

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

#define PST_META_CHILDREN(LT)                                                                                          \
	LT(pst_path, LogicalType::VARCHAR)                                                                                 \
	LT(pst_name, LogicalType::VARCHAR)

enum class PSTMetaProjection { PST_META_CHILDREN(SCHEMA_CHILD_NAME) };

// These are MAPI attributes shared by all objects; they form the base row of a PST read
#define COMMON_CHILDREN(LT)                                                                                            \
	LT(entry_id, LogicalType::BLOB)                                                                                    \
	LT(parent_entry_id, LogicalType::BLOB)                                                                             \
	LT(display_name, LogicalType::VARCHAR)                                                                             \
	LT(comment, LogicalType::VARCHAR)                                                                                  \
	LT(creation_time, LogicalType::TIMESTAMP_S)                                                                        \
	LT(last_modified, LogicalType::TIMESTAMP_S)

enum class CommonProjection { COMMON_CHILDREN(SCHEMA_CHILD_NAME) NUM_FIELDS };

static const auto COMMON_SCHEMA = LogicalType::STRUCT({COMMON_CHILDREN(SCHEMA_CHILD)});

enum class CommonWithPSTProjection { PST_META_CHILDREN(SCHEMA_CHILD_NAME) COMMON_CHILDREN(SCHEMA_CHILD_NAME) };

#define RECIPIENT_CHILDREN(LT)                                                                                         \
	LT(account_name, LogicalType::VARCHAR)                                                                             \
	LT(email_address, LogicalType::VARCHAR)                                                                            \
	LT(address_type, LogicalType::VARCHAR)                                                                             \
	LT(recipient_type, RECIPIENT_TYPE_ENUM)                                                                            \
	LT(recipient_type_raw, LogicalType::INTEGER)

enum class RecipientProjection { COMMON_CHILDREN(SCHEMA_CHILD_NAME) RECIPIENT_CHILDREN(SCHEMA_CHILD_NAME) };

static const auto RECIPIENT_SCHEMA =
    LogicalType::STRUCT({COMMON_CHILDREN(SCHEMA_CHILD) RECIPIENT_CHILDREN(SCHEMA_CHILD)});

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

#define MESSAGE_CHILDREN(LT)                                                                                           \
	LT(message_id, LogicalType::UINTEGER)                                                                              \
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
	PST_META_CHILDREN(SCHEMA_CHILD_NAME) COMMON_CHILDREN(SCHEMA_CHILD_NAME) MESSAGE_CHILDREN(SCHEMA_CHILD_NAME)
};

static const auto MESSAGE_SCHEMA =
    LogicalType::STRUCT({PST_META_CHILDREN(SCHEMA_CHILD) COMMON_CHILDREN(SCHEMA_CHILD) MESSAGE_CHILDREN(SCHEMA_CHILD)});

#define FOLDER_CHILDREN(LT)                                                                                            \
	LT(parent_folder_id, LogicalType::UINTEGER)                                                                        \
	LT(folder_id, LogicalType::UINTEGER)                                                                               \
	LT(folder_name, LogicalType::VARCHAR)                                                                              \
	LT(subfolder_count, LogicalType::UINTEGER)                                                                         \
	LT(message_count, LogicalType::BIGINT)                                                                             \
	LT(unread_message_count, LogicalType::BIGINT)

enum class FolderProjection {
	PST_META_CHILDREN(SCHEMA_CHILD_NAME) COMMON_CHILDREN(SCHEMA_CHILD_NAME) FOLDER_CHILDREN(SCHEMA_CHILD_NAME)
};

static const auto FOLDER_SCHEMA =
    LogicalType::STRUCT({PST_META_CHILDREN(SCHEMA_CHILD) COMMON_CHILDREN(SCHEMA_CHILD) FOLDER_CHILDREN(SCHEMA_CHILD)});
} // namespace intellekt::duckpst::schema