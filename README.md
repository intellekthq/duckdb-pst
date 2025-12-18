<img width="100" alt="logo" src="https://github.com/user-attachments/assets/279fa223-f786-4e95-9d8f-1d0d9276afce" />

# duckdb-pst
A DuckDB extension for reading [Microsoft PST files](https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-pst/141923d5-15ab-4ef1-a524-6dce75aae546) with rich schemas for common MAPI types, built on Microsoft's official PST SDK. Query emails, contacts, calendar appointments, and tasks from PST archives using plain SQL—directly from local disk or any DuckDB-supported object storage provider (S3, Azure, GCS, etc.). Use it to analyze PST data in-place, import to DuckDB tables (or any other supported federated write provider, e.g. Parquet or Delta Table).

## Getting Started

Quickly count all messages or folders in a directory full of PSTs (this one has 167 files):

```sql
memory D select count(*) from read_pst_messages('enron/*.pst');
┌────────────────┐
│  count_star()  │
│     int64      │
├────────────────┤
│    1167830     │
│ (1.17 million) │
└────────────────┘
Run Time (s): real 0.564 user 0.381726 sys 0.701447
```

What kinds of objects are in this PST?

```sql
select message_class, count(*) as c from read_pst_messages('test/*.pst') group by message_class order by c desc;
┌─────────────────┬───────┐
│  message_class  │   c   │
│     varchar     │ int64 │
├─────────────────┼───────┤
│ IPM.Note        │     5 │
│ IPM.Contact     │     2 │
│ IPM.StickyNote  │     2 │
│ IPM.DistList    │     1 │
│ IPM.Task        │     1 │
│ IPM.Appointment │     1 │
└─────────────────┴───────┘
```

Read the first 5 messages (with limit applied during planning -- for large files):

```sql
memory D select * from read_pst_messages('enron/*.pst', read_limit=5);
┌────────────────────────┬────────────────┬──────────────────────┬─────────┬───┬──────────────┬──────────────────────┬──────────────────────┐
│        pst_path        │    pst_name    │      record_key      │ node_id │ … │ message_size │  conversation_topic  │ internet_message_id  │
│        varchar         │    varchar     │         blob         │ uint32  │ … │    uint64    │       varchar        │       varchar        │
├────────────────────────┼────────────────┼──────────────────────┼─────────┼───┼──────────────┼──────────────────────┼──────────────────────┤
│ enron/zl_delainey-d_0… │ delainey-d_000 │ \xD5\xEF\xD5\x86\x8… │ 2097316 │ … │        15603 │ Construction Servic… │ <8a905b300bb6184fa2… │
│ enron/zl_delainey-d_0… │ delainey-d_000 │ \xD5\xEF\xD5\x86\x8… │ 2097284 │ … │         2671 │ Mike McConnell 3-14… │ <fcd814da82d47f4795… │
│ enron/zl_delainey-d_0… │ delainey-d_000 │ \xD5\xEF\xD5\x86\x8… │ 2097252 │ … │         2641 │ Mark Lindsey Mtg     │ <fcd814da82d47f4795… │
│ enron/zl_delainey-d_0… │ delainey-d_000 │ \xD5\xEF\xD5\x86\x8… │ 2097220 │ … │         2659 │ Jim Fallon (called)  │ <fcd814da82d47f4795… │
│ enron/zl_delainey-d_0… │ delainey-d_000 │ \xD5\xEF\xD5\x86\x8… │ 2097188 │ … │         2653 │ Call Jeff Shankman   │ <fcd814da82d47f4795… │
├────────────────────────┴────────────────┴──────────────────────┴─────────┴───┴──────────────┴──────────────────────┴──────────────────────┤
│ 5 rows                                                                                                               26 columns (7 shown) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
Run Time (s): real 0.012 user 0.015968 sys 0.015498
```

## Performance Features

PSTs have many database-like properties, allowing us to leverage advanced DuckDB features to enable performant reads:

- **Query pushdown**: projection, statistics, and filter pushdown for efficient scans
- **Concurrent planning**: parallel partition planning for directories with many PST files
- **Late materialization**: filter on virtual columns before expanding full projections (WIP)
- **Progress tracking**: built-in progress API for monitoring large scans

## Usage

The extension provides specialized table functions for different MAPI message types:

**`read_pst_messages`** - Returns all messages with the base `IPM.Note` schema. Use this for aggregate queries or when you need all message types (check the `message_class` column to determine specific types).

**Type-specific functions** (`read_pst_contacts`, `read_pst_appointments`, etc.) - Filter messages by type during query planning. These inherit all base `IPM.Note` fields plus additional fields for their specific type. Planning is slower due to filtering (unfortunately, this requires a comparison of the `PR_MESSAGE_CLASS` string), but you get a richer schema and reduced result set. 

| Table Function            | MAPI Message Class  | Description                                              |
|---------------------------|---------------------|----------------------------------------------------------|
| `read_pst_folders`        | `*`                 | Folders                                                  |
| `read_pst_messages`       | `*`                 | All messages, with base `IPM.Note` email projection      |
| `read_pst_notes`          | `IPM.Note`          | (Filtered) only `IPM.Note` (and unimplemented types)     |
| `read_pst_contacts`       | `IPM.Contact`       | (Filtered) only contacts with contact-specific fields    |
| `read_pst_appointments`   | `IPM.Appointment`   | (Filtered) only calendar appointments and meetings       |
| `read_pst_sticky_notes`   | `IPM.StickyNote`    | (Filtered) only sticky note items                        |
| `read_pst_tasks`          | `IPM.Task`          | (Filtered) task items with task-specific fields          |

## Schemas

All table functions return PST metadata fields. Message-based functions inherit base `IPM.Note` fields plus type-specific additions.

**Table of Contents:**
- [Common PST Metadata](#common-pst-metadata-all-functions) - Fields present in all table functions
- [Folders](#folders-read_pst_folders) - Folder-specific fields
- [Base Messages](#base-messages-read_pst_messages-read_pst_notes) - Core message fields (emails, notes)
- [Contacts](#contacts-read_pst_contacts) - Contact-specific fields
- [Appointments](#appointments-read_pst_appointments) - Calendar/meeting fields
- [Sticky Notes](#sticky-notes-read_pst_sticky_notes) - Sticky note fields
- [Tasks](#tasks-read_pst_tasks) - Task management fields

### Common PST Metadata (all functions)

| Field             | Type        | Description                        |
|-------------------|-------------|------------------------------------|
| `pst_path`        | `VARCHAR`   | Full path to the PST file          |
| `pst_name`        | `VARCHAR`   | PST filename without extension     |
| `record_key`      | `BLOB`      | Unique record identifier           |
| `node_id`         | `UINTEGER`  | Node ID within PST                 |
| `parent_node_id`  | `UINTEGER`  | Parent node ID                     |

### Folders (`read_pst_folders`)

Includes PST metadata plus:

| Field                   | Type        | Description                  |
|-------------------------|-------------|------------------------------|
| `container_class`       | `VARCHAR`   | Folder container class       |
| `display_name`          | `VARCHAR`   | Folder display name          |
| `subfolder_count`       | `UINTEGER`  | Number of subfolders         |
| `message_count`         | `BIGINT`    | Total messages in folder     |
| `unread_message_count`  | `BIGINT`    | Unread message count         |

### Base Messages (`read_pst_messages`, `read_pst_notes`)

Includes PST metadata plus:

| Field                    | Type            | Description                                                                      |
|--------------------------|-----------------|----------------------------------------------------------------------------------|
| `subject`                | `VARCHAR`       | Message subject                                                                  |
| `body`                   | `VARCHAR`       | Plain text body                                                                  |
| `body_html`              | `VARCHAR`       | HTML body                                                                        |
| `display_name`           | `VARCHAR`       | Display name                                                                     |
| `comment`                | `VARCHAR`       | Comment field                                                                    |
| `sender_name`            | `VARCHAR`       | Sender display name                                                              |
| `sender_email_address`   | `VARCHAR`       | Sender email                                                                     |
| `recipients`             | `LIST(STRUCT)`  | List of recipients with `display_name`, `email_address`, `recipient_type`, etc.  |
| `has_attachments`        | `BOOLEAN`       | Whether message has attachments                                                  |
| `attachment_count`       | `UINTEGER`      | Number of attachments                                                            |
| `attachments`            | `LIST(STRUCT)`  | List of attachments with `filename`, `mime_type`, `size`, `bytes`, etc.          |
| `importance`             | `ENUM`          | Message importance: `LOW`, `NORMAL`, `HIGH`                                      |
| `priority`               | `ENUM`          | Message priority: `NONURGENT`, `NORMAL`, `URGENT`                                |
| `sensitivity`            | `ENUM`          | Message sensitivity: `NONE`, `PERSONAL`, `PRIVATE`, `CONFIDENTIAL`               |
| `creation_time`          | `TIMESTAMP_S`   | Creation timestamp                                                               |
| `last_modified`          | `TIMESTAMP_S`   | Last modification timestamp                                                      |
| `message_delivery_time`  | `TIMESTAMP_S`   | Delivery timestamp                                                               |
| `message_class`          | `VARCHAR`       | MAPI message class (e.g., `IPM.Note`, `IPM.Contact`)                             |
| `message_flags`          | `INTEGER`       | Message flags                                                                    |
| `message_size`           | `UBIGINT`       | Message size in bytes                                                            |
| `conversation_topic`     | `VARCHAR`       | Conversation topic                                                               |
| `internet_message_id`    | `VARCHAR`       | Internet message ID                                                              |

### Contacts (`read_pst_contacts`)

Includes PST metadata + base message fields + contact-specific fields:

<details>
<summary>78 contact-specific fields (click to expand)</summary>

| Field                          | Type           | Description                   |
|--------------------------------|----------------|-------------------------------|
| `display_name_prefix`          | `VARCHAR`      | Name prefix (Mr., Mrs., etc.) |
| `given_name`                   | `VARCHAR`      | First name                    |
| `middle_name`                  | `VARCHAR`      | Middle name                   |
| `surname`                      | `VARCHAR`      | Last name                     |
| `generation_suffix`            | `VARCHAR`      | Suffix (Jr., Sr., etc.)       |
| `initials`                     | `VARCHAR`      | Initials                      |
| `nickname`                     | `VARCHAR`      | Nickname                      |
| `preferred_by_name`            | `VARCHAR`      | Preferred name                |
| `account_name`                 | `VARCHAR`      | Account name                  |
| `original_display_name`        | `VARCHAR`      | Original display name         |
| `transmittable_display_name`   | `VARCHAR`      | Transmittable display name    |
| `mhs_common_name`              | `VARCHAR`      | MHS common name               |
| `government_id_number`         | `VARCHAR`      | Government ID number          |
| `organizational_id_number`     | `VARCHAR`      | Organizational ID number      |
| `birthday`                     | `TIMESTAMP_S`  | Birthday                      |
| `wedding_anniversary`          | `TIMESTAMP_S`  | Wedding anniversary           |
| `spouse_name`                  | `VARCHAR`      | Spouse's name                 |
| `childrens_names`              | `VARCHAR`      | Children's names              |
| `gender`                       | `SMALLINT`     | Gender                        |
| `hobbies`                      | `VARCHAR`      | Hobbies                       |
| `profession`                   | `VARCHAR`      | Profession                    |
| `language`                     | `VARCHAR`      | Language                      |
| `location`                     | `VARCHAR`      | Location                      |
| `keyword`                      | `VARCHAR`      | Keyword                       |
| `company_name`                 | `VARCHAR`      | Company name                  |
| `title`                        | `VARCHAR`      | Job title                     |
| `department_name`              | `VARCHAR`      | Department name               |
| `office_location`              | `VARCHAR`      | Office location               |
| `manager_name`                 | `VARCHAR`      | Manager's name                |
| `assistant`                    | `VARCHAR`      | Assistant's name              |
| `customer_id`                  | `VARCHAR`      | Customer ID                   |
| `primary_telephone`            | `VARCHAR`      | Primary telephone             |
| `business_telephone`           | `VARCHAR`      | Business telephone            |
| `business_telephone_2`         | `VARCHAR`      | Business telephone 2          |
| `home_telephone`               | `VARCHAR`      | Home telephone                |
| `home_telephone_2`             | `VARCHAR`      | Home telephone 2              |
| `mobile_telephone`             | `VARCHAR`      | Mobile telephone              |
| `car_telephone`                | `VARCHAR`      | Car telephone                 |
| `radio_telephone`              | `VARCHAR`      | Radio telephone               |
| `pager_telephone`              | `VARCHAR`      | Pager telephone               |
| `callback_number`              | `VARCHAR`      | Callback number               |
| `other_telephone`              | `VARCHAR`      | Other telephone               |
| `assistant_telephone`          | `VARCHAR`      | Assistant telephone           |
| `company_main_phone`           | `VARCHAR`      | Company main phone            |
| `ttytdd_phone`                 | `VARCHAR`      | TTY/TDD phone                 |
| `isdn_number`                  | `VARCHAR`      | ISDN number                   |
| `telex_number`                 | `VARCHAR`      | Telex number                  |
| `primary_fax`                  | `VARCHAR`      | Primary fax                   |
| `business_fax`                 | `VARCHAR`      | Business fax                  |
| `home_fax`                     | `VARCHAR`      | Home fax                      |
| `business_address_street`      | `VARCHAR`      | Business street address       |
| `business_address_city`        | `VARCHAR`      | Business city                 |
| `business_address_state`       | `VARCHAR`      | Business state                |
| `business_postal_code`         | `VARCHAR`      | Business postal code          |
| `business_address_country`     | `VARCHAR`      | Business country              |
| `business_po_box`              | `VARCHAR`      | Business PO box               |
| `home_address_street`          | `VARCHAR`      | Home street address           |
| `home_address_city`            | `VARCHAR`      | Home city                     |
| `home_address_state`           | `VARCHAR`      | Home state                    |
| `home_address_postal_code`     | `VARCHAR`      | Home postal code              |
| `home_address_country`         | `VARCHAR`      | Home country                  |
| `home_address_po_box`          | `VARCHAR`      | Home PO box                   |
| `other_address_street`         | `VARCHAR`      | Other street address          |
| `other_address_city`           | `VARCHAR`      | Other city                    |
| `other_address_state`          | `VARCHAR`      | Other state                   |
| `other_address_postal_code`    | `VARCHAR`      | Other postal code             |
| `other_address_country`        | `VARCHAR`      | Other country                 |
| `other_address_po_box`         | `VARCHAR`      | Other PO box                  |
| `postal_address`               | `VARCHAR`      | Postal address                |
| `personal_home_page`           | `VARCHAR`      | Personal home page            |
| `business_home_page`           | `VARCHAR`      | Business home page            |
| `ftp_site`                     | `VARCHAR`      | FTP site                      |
| `computer_network_name`        | `VARCHAR`      | Computer network name         |
| `mail_permission`              | `BOOLEAN`      | Mail permission               |
| `send_rich_info`               | `BOOLEAN`      | Send rich info                |
| `conversation_prohibited`      | `BOOLEAN`      | Conversation prohibited       |
| `disclose_recipients`          | `BOOLEAN`      | Disclose recipients           |

</details>

### Appointments (`read_pst_appointments`)

Includes PST metadata + base message fields + appointment-specific fields:

| Field                    | Type           | Description              |
|--------------------------|----------------|--------------------------|
| `location`               | `VARCHAR`      | Meeting location         |
| `start_time`             | `TIMESTAMP_S`  | Start time               |
| `end_time`               | `TIMESTAMP_S`  | End time                 |
| `duration`               | `INTEGER`      | Duration in minutes      |
| `all_day_event`          | `BOOLEAN`      | All-day event flag       |
| `is_meeting`             | `BOOLEAN`      | Is a meeting             |
| `organizer_name`         | `VARCHAR`      | Organizer's name         |
| `required_attendees`     | `VARCHAR`      | Required attendees       |
| `optional_attendees`     | `VARCHAR`      | Optional attendees       |
| `meeting_workspace_url`  | `VARCHAR`      | Meeting workspace URL    |
| `busy_status`            | `INTEGER`      | Busy status              |
| `response_status`        | `INTEGER`      | Response status          |
| `is_recurring`           | `BOOLEAN`      | Recurring event flag     |
| `recurrence_pattern`     | `VARCHAR`      | Recurrence pattern       |
| `is_private`             | `BOOLEAN`      | Private event flag       |

### Sticky Notes (`read_pst_sticky_notes`)

Includes PST metadata + base message fields + sticky note-specific fields:

| Field         | Type       | Description      |
|---------------|------------|------------------|
| `note_color`  | `INTEGER`  | Note color code  |
| `note_width`  | `INTEGER`  | Note width       |
| `note_height` | `INTEGER`  | Note height      |
| `note_x`      | `INTEGER`  | X position       |
| `note_y`      | `INTEGER`  | Y position       |

### Tasks (`read_pst_tasks`)

Includes PST metadata + base message fields + task-specific fields:

| Field               | Type           | Description                    |
|---------------------|----------------|--------------------------------|
| `task_status`       | `INTEGER`      | Task status code               |
| `is_complete`       | `BOOLEAN`      | Completion flag                |
| `percent_complete`  | `DOUBLE`       | Completion percentage          |
| `start_date`        | `TIMESTAMP_S`  | Start date                     |
| `due_date`          | `TIMESTAMP_S`  | Due date                       |
| `date_completed`    | `TIMESTAMP_S`  | Completion date                |
| `last_update`       | `TIMESTAMP_S`  | Last update time               |
| `estimated_effort`  | `INTEGER`      | Estimated effort (minutes)     |
| `actual_effort`     | `INTEGER`      | Actual effort (minutes)        |
| `task_owner`        | `VARCHAR`      | Task owner                     |
| `task_assigner`     | `VARCHAR`      | Task assigner                  |
| `ownership`         | `INTEGER`      | Ownership code                 |
| `last_user`         | `VARCHAR`      | Last user to modify            |
| `is_team_task`      | `BOOLEAN`      | Team task flag                 |
| `is_recurring`      | `BOOLEAN`      | Recurring task flag            |

## Building

```bash
git submodule update --init --recursive
# GEN=ninja make release
# Build with UI extension
GEN=ninja make debug
./build/debug/duckdb -ui
```

## Credits

This extension is built on top of the [microsoft-pst-sdk](https://github.com/terrymah/microsoft-pst-sdk) by [Terry Mahaffey](https://github.com/terrymah), Microsoft's official C++ SDK for reading PST files.
