#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "pstsdk/pst/message.h"

// Everything that we want to emit as a row or column
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

static const auto RECIPIENT_SCHEMA = LogicalType::STRUCT({{"name", LogicalType::VARCHAR},
                                                          {"account_name", LogicalType::VARCHAR},
                                                          {"email_address", LogicalType::VARCHAR},
                                                          {"address_type", LogicalType::VARCHAR},
                                                          {"recipient_type", RECIPIENT_TYPE_ENUM}});

static const auto ATTACHMENT_SCHEMA = LogicalType::STRUCT({{"filename", LogicalType::VARCHAR},
                                                           {"long_filename", LogicalType::VARCHAR},
                                                           {"extension", LogicalType::VARCHAR},
                                                           {"mime_type", LogicalType::VARCHAR},
                                                           {"size", LogicalType::UBIGINT},
                                                           {"is_message", LogicalType::BOOLEAN},
                                                           {"content_id", LogicalType::VARCHAR},
                                                           {"attach_method", ATTACH_METHOD_ENUM}});

static const auto MESSAGE_SCHEMA = LogicalType::STRUCT({{"pst_path", LogicalType::VARCHAR},
                                                        {"pst_name", LogicalType::VARCHAR},
                                                        {"folder_id", LogicalType::UINTEGER},
                                                        {"message_id", LogicalType::UINTEGER},
                                                        {"subject", LogicalType::VARCHAR},
                                                        {"sender_name", LogicalType::VARCHAR},
                                                        {"sender_email_address", LogicalType::VARCHAR},
                                                        {"message_delivery_time", LogicalType::TIMESTAMP},
                                                        {"message_class", LogicalType::VARCHAR},
                                                        {"importance", IMPORTANCE_ENUM},
                                                        {"sensitivity", SENSITIVITY_ENUM},
                                                        {"message_flags", LogicalType::UINTEGER},
                                                        {"message_size", LogicalType::UINTEGER},
                                                        {"has_attachments", LogicalType::BOOLEAN},
                                                        {"attachment_count", LogicalType::UINTEGER},
                                                        {"body", LogicalType::VARCHAR},
                                                        {"body_html", LogicalType::VARCHAR},
                                                        {"internet_message_id", LogicalType::VARCHAR},
                                                        {"conversation_topic", LogicalType::VARCHAR},
                                                        {"recipients", LogicalType::LIST(RECIPIENT_SCHEMA)},
                                                        {"attachments", LogicalType::LIST(ATTACHMENT_SCHEMA)}});

static const auto FOLDER_SCHEMA = LogicalType::STRUCT({
    {"pst_path", LogicalType::VARCHAR},
    {"pst_name", LogicalType::VARCHAR},
    {"parent_folder_id", LogicalType::UINTEGER},
    {"folder_id", LogicalType::UINTEGER},
    {"folder_name", LogicalType::VARCHAR},
    {"subfolder_count", LogicalType::UINTEGER},
    {"message_count", LogicalType::BIGINT},
    {"unread_message_count", LogicalType::BIGINT},
});

template <typename Item>
void into_row(DataChunk &output, Item &item, idx_t row_number);

} // namespace intellekt::duckpst::schema
