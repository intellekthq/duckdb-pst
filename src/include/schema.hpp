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

/* Contact schema */

#define CONTACT_CHILDREN(LT)                                                                                           \
	LT(account_name, LogicalType::VARCHAR)                                                                             \
	LT(callback_number, LogicalType::VARCHAR)                                                                          \
	LT(conversation_prohibited, LogicalType::BOOLEAN)                                                                  \
	LT(disclose_recipients, LogicalType::BOOLEAN)                                                                      \
	LT(generation_suffix, LogicalType::VARCHAR)                                                                        \
	LT(given_name, LogicalType::VARCHAR)                                                                               \
	LT(government_id_number, LogicalType::VARCHAR)                                                                     \
	LT(business_telephone, LogicalType::VARCHAR)                                                                       \
	LT(home_telephone, LogicalType::VARCHAR)                                                                           \
	LT(initials, LogicalType::VARCHAR)                                                                                 \
	LT(keyword, LogicalType::VARCHAR)                                                                                  \
	LT(language, LogicalType::VARCHAR)                                                                                 \
	LT(location, LogicalType::VARCHAR)                                                                                 \
	LT(mail_permission, LogicalType::BOOLEAN)                                                                          \
	LT(mhs_common_name, LogicalType::VARCHAR)                                                                          \
	LT(organizational_id_number, LogicalType::VARCHAR)                                                                 \
	LT(surname, LogicalType::VARCHAR)                                                                                  \
	LT(original_display_name, LogicalType::VARCHAR)                                                                    \
	LT(postal_address, LogicalType::VARCHAR)                                                                           \
	LT(company_name, LogicalType::VARCHAR)                                                                             \
	LT(title, LogicalType::VARCHAR)                                                                                    \
	LT(department_name, LogicalType::VARCHAR)                                                                          \
	LT(office_location, LogicalType::VARCHAR)                                                                          \
	LT(primary_telephone, LogicalType::VARCHAR)                                                                        \
	LT(business_telephone_2, LogicalType::VARCHAR)                                                                     \
	LT(mobile_telephone, LogicalType::VARCHAR)                                                                         \
	LT(radio_telephone, LogicalType::VARCHAR)                                                                          \
	LT(car_telephone, LogicalType::VARCHAR)                                                                            \
	LT(other_telephone, LogicalType::VARCHAR)                                                                          \
	LT(transmittable_display_name, LogicalType::VARCHAR)                                                               \
	LT(pager_telephone, LogicalType::VARCHAR)                                                                          \
	LT(primary_fax, LogicalType::VARCHAR)                                                                              \
	LT(business_fax, LogicalType::VARCHAR)                                                                             \
	LT(home_fax, LogicalType::VARCHAR)                                                                                 \
	LT(business_address_country, LogicalType::VARCHAR)                                                                 \
	LT(business_address_city, LogicalType::VARCHAR)                                                                    \
	LT(business_address_state, LogicalType::VARCHAR)                                                                   \
	LT(business_address_street, LogicalType::VARCHAR)                                                                  \
	LT(business_postal_code, LogicalType::VARCHAR)                                                                     \
	LT(business_po_box, LogicalType::VARCHAR)                                                                          \
	LT(telex_number, LogicalType::VARCHAR)                                                                             \
	LT(isdn_number, LogicalType::VARCHAR)                                                                              \
	LT(assistant_telephone, LogicalType::VARCHAR)                                                                      \
	LT(home_telephone_2, LogicalType::VARCHAR)                                                                         \
	LT(assistant, LogicalType::VARCHAR)                                                                                \
	LT(send_rich_info, LogicalType::BOOLEAN)                                                                           \
	LT(wedding_anniversary, LogicalType::TIMESTAMP_S)                                                                  \
	LT(birthday, LogicalType::TIMESTAMP_S)                                                                             \
	LT(hobbies, LogicalType::VARCHAR)                                                                                  \
	LT(middle_name, LogicalType::VARCHAR)                                                                              \
	LT(display_name_prefix, LogicalType::VARCHAR)                                                                      \
	LT(profession, LogicalType::VARCHAR)                                                                               \
	LT(preferred_by_name, LogicalType::VARCHAR)                                                                        \
	LT(spouse_name, LogicalType::VARCHAR)                                                                              \
	LT(computer_network_name, LogicalType::VARCHAR)                                                                    \
	LT(customer_id, LogicalType::VARCHAR)                                                                              \
	LT(ttytdd_phone, LogicalType::VARCHAR)                                                                             \
	LT(ftp_site, LogicalType::VARCHAR)                                                                                 \
	LT(gender, LogicalType::SMALLINT)                                                                                  \
	LT(manager_name, LogicalType::VARCHAR)                                                                             \
	LT(nickname, LogicalType::VARCHAR)                                                                                 \
	LT(personal_home_page, LogicalType::VARCHAR)                                                                       \
	LT(business_home_page, LogicalType::VARCHAR)                                                                       \
	LT(company_main_phone, LogicalType::VARCHAR)                                                                       \
	LT(childrens_names, LogicalType::VARCHAR)                                                                          \
	LT(home_address_city, LogicalType::VARCHAR)                                                                        \
	LT(home_address_country, LogicalType::VARCHAR)                                                                     \
	LT(home_address_postal_code, LogicalType::VARCHAR)                                                                 \
	LT(home_address_state, LogicalType::VARCHAR)                                                                       \
	LT(home_address_street, LogicalType::VARCHAR)                                                                      \
	LT(home_address_po_box, LogicalType::VARCHAR)                                                                      \
	LT(other_address_city, LogicalType::VARCHAR)                                                                       \
	LT(other_address_country, LogicalType::VARCHAR)                                                                    \
	LT(other_address_postal_code, LogicalType::VARCHAR)                                                                \
	LT(other_address_state, LogicalType::VARCHAR)                                                                      \
	LT(other_address_street, LogicalType::VARCHAR)                                                                     \
	LT(other_address_po_box, LogicalType::VARCHAR)

enum class ContactProjection {
	PST_CHILDREN(SCHEMA_CHILD_NAME) COMMON_CHILDREN(SCHEMA_CHILD_NAME) CONTACT_CHILDREN(SCHEMA_CHILD_NAME)
};

static const auto CONTACT_SCHEMA =
    LogicalType::STRUCT({PST_CHILDREN(SCHEMA_CHILD) COMMON_CHILDREN(SCHEMA_CHILD) CONTACT_CHILDREN(SCHEMA_CHILD)});

/* Appointment schema */
#define APPOINTMENT_CHILDREN(LT)                                                                                       \
	LT(location, LogicalType::VARCHAR)                                                                                 \
	LT(start_time, LogicalType::TIMESTAMP_S)                                                                           \
	LT(end_time, LogicalType::TIMESTAMP_S)                                                                             \
	LT(duration, LogicalType::INTEGER)                                                                                 \
	LT(all_day_event, LogicalType::BOOLEAN)                                                                            \
	LT(busy_status, LogicalType::INTEGER)                                                                              \
	LT(meeting_workspace_url, LogicalType::VARCHAR)                                                                    \
	LT(organizer_name, LogicalType::VARCHAR)                                                                           \
	LT(required_attendees, LogicalType::VARCHAR)                                                                       \
	LT(optional_attendees, LogicalType::VARCHAR)                                                                       \
	LT(is_recurring, LogicalType::BOOLEAN)                                                                             \
	LT(recurrence_pattern, LogicalType::VARCHAR)                                                                       \
	LT(is_private, LogicalType::BOOLEAN)                                                                               \
	LT(response_status, LogicalType::INTEGER)                                                                          \
	LT(is_meeting, LogicalType::BOOLEAN)

enum class AppointmentProjection {
	PST_CHILDREN(SCHEMA_CHILD_NAME) COMMON_CHILDREN(SCHEMA_CHILD_NAME) APPOINTMENT_CHILDREN(SCHEMA_CHILD_NAME)
};

static const auto APPOINTMENT_SCHEMA =
    LogicalType::STRUCT({PST_CHILDREN(SCHEMA_CHILD) COMMON_CHILDREN(SCHEMA_CHILD) APPOINTMENT_CHILDREN(SCHEMA_CHILD)});

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