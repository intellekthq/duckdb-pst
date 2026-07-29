<img width="100" alt="logo" src="https://github.com/user-attachments/assets/279fa223-f786-4e95-9d8f-1d0d9276afce" />

# duckdb-pst

[![Build](https://img.shields.io/github/actions/workflow/status/intellekthq/duckdb-pst/MainDistributionPipeline.yml?label=build)](https://github.com/intellekthq/duckdb-pst/actions/workflows/MainDistributionPipeline.yml)

A DuckDB extension for reading [Microsoft PST files](https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-pst/141923d5-15ab-4ef1-a524-6dce75aae546) with rich schemas for common MAPI types, built on Microsoft's official PST SDK. Query emails, contacts, appointments (and others). Use it to analyze PST data in-place (locally, or on object storage), import to DuckDB tables, or export to Parquet.

It can also [delete](#deleting) from a PST in place: scrub PII, drop out-of-scope custodian mail before a production, or strip attachments.

## Getting Started

Quickly count all messages or folders in a directory full of PSTs (171 files, 77.4 GiB):

```sql
D select count(*) from read_pst_messages('enron/*.pst');
┌────────────────┐
│  count_star()  │
│     int64      │
├────────────────┤
│    1227193     │
│ (1.23 million) │
└────────────────┘
Run Time (s): real 0.534 user 0.466194 sys 0.650549
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

Have a remote URI for your PST? No problem:

```sql
select given_name, surname from read_pst_contacts('https://github.com/intellekthq/duckdb-pst/raw/refs/heads/main/test/unittest.pst');
┌────────────┬─────────┐
│ given_name │ surname │
│  varchar   │ varchar │
├────────────┼─────────┤
│ Hopper     │ Cat     │
│ Linus      │ Cat     │
└────────────┴─────────┘
```

See [Common Queries](#common-queries) for more examples of common PST analysis tasks.

## Performance Features

PSTs have many database-like properties, allowing us to leverage advanced DuckDB features to enable performant reads:

- **Query pushdown**: projection and statistics pushdown
- **Concurrent planning**: parallel partition planning for directories with many PST files
- **Late materialization**: filter on virtual columns before expanding full projections ([example](#find-messages-matching-a-conversation-topic))
- **Progress tracking**: implements progress API for monitoring large scans

## Usage

The extension provides specialized table functions for different MAPI message types:

**`read_pst_messages`** - Returns all messages with the base `IPM.Note` schema. Use this for aggregate queries or when you need all message types (check the `message_class` column to determine specific types).

**Type-specific functions** (`read_pst_contacts`, `read_pst_appointments`, etc.) - Filter messages by type during query planning. These inherit all base `IPM.Note` fields plus additional fields for their specific type. Planning is slower due to filtering (unfortunately, this requires a comparison of the `PR_MESSAGE_CLASS` string), but you get a richer schema and reduced result set. 

| Table Function                | MAPI Message Class  | Description                                              |
|-------------------------------|---------------------|----------------------------------------------------------|
| `read_pst_folders`            | `*`                 | Folders                                                  |
| `read_pst_messages`           | `*`                 | All messages, with base `IPM.Note` email projection      |
| `read_pst_notes`              | `IPM.Note`          | (Filtered) only `IPM.Note` (and unimplemented types)     |
| `read_pst_contacts`           | `IPM.Contact`       | (Filtered) only contacts with contact-specific fields    |
| `read_pst_distribution_lists` | `IPM.DistList`      | (Filtered) distribution lists with member information    |
| `read_pst_appointments`       | `IPM.Appointment`   | (Filtered) only calendar appointments and meetings       |
| `read_pst_sticky_notes`       | `IPM.StickyNote`    | (Filtered) only sticky note items                        |
| `read_pst_tasks`              | `IPM.Task`          | (Filtered) task items with task-specific fields          |

To observe any read issues, enable the DuckDB logger. Errors typically reference an individual file or column:

```sql
CALL enable_logging(level = 'debug');
SELECT * from duckdb_logs();
```

**Note:** Large directories with large PST files may trip your `ulimit`. Check the logs and adjust accordingly. If unable to adjust the system ulimit, you can try using the `planning_concurrency` parameter, or DuckDB's `SET threads = n;`.

### Function Parameters

All table functions accept the following named parameters. Note that **by default** message bodies are truncated to 1M and attachment contents are not read.

| Parameter              | Default      | Description                                                                        |
|------------------------|--------------|------------------------------------------------------------------------------------|
| `read_body_size_bytes` | `1000000`    | Maximum bytes to read into `body` and `body_html`. Set to 0 to read all.           |
| `read_attachment_body` | `false`      | Whether to read attachment bytes into the `bytes` field                            |
| `read_limit`           | `NULL`       | Maximum number of items to read (applied during planning, stops crawling fs)       |
| `planning_concurrency` | `UINT32_MAX` | Maximum concurrent async tasks during partition planning (applies when globbing multiple PST files). |

## Deleting

The delete functions edit a PST in place. There is no copy, no temporary file, no transaction and no rollback, so a delete cannot be undone. Freed blocks are zeroed on the way out, so what you delete is gone rather than just unreachable.

The file does not shrink. Freed space goes back to the store's own allocator for the next write, not to the filesystem.

Deletion is **disabled by default**. Both switches are required:

| Switch                        | Scope   | Effect                                              |
|-------------------------------|---------|-----------------------------------------------------|
| `SET pst_allow_delete = true` | Session | Permits `really := true`                            |
| `really := true`              | Call    | Deletes, instead of previewing                      |

Without `really`, the call previews: it opens the store read-only, resolves every target, and reports `PREVIEWED` or `FAILED` per row. Previews are not gated, and `bytes_wiped` on a previewed wipe is `NULL`.

**Note:** DuckDB does not enforce the declared scope of an extension setting. `SET GLOBAL pst_allow_delete = true` therefore arms every connection on the database, and a plain `RESET` clears only the connection that ran it. Set it per session, and assume a pooled connection is already armed if anything in the process ever set it globally.

| Table Function            | Input Table                                                                 | Deletes                                         |
|---------------------------|-----------------------------------------------------------------------------|-------------------------------------------------|
| `delete_pst_messages`     | `(pst_path VARCHAR, node_id UINTEGER)`                                      | One message, and its attachments                |
| `delete_pst_folders`      | `(pst_path VARCHAR, node_id UINTEGER)`                                      | A folder, its subfolders and everything in them |
| `delete_pst_attachments`  | `(pst_path VARCHAR, message_node_id UINTEGER, attachment_node_id UINTEGER)` | One attachment, leaving the message             |
| `wipe_pst_free_space`     | a path or glob, not a table                                                 | Overwrites bytes no node points at              |

There are no per message-class delete functions; filter by class in the subquery.

Targets come from a nested read. The input table is positional:

```sql
set pst_allow_delete = true;

select * from delete_pst_messages(
  (select pst_path, node_id from read_pst_messages('/tmp/doc/hr.pst')
    where message_class = 'IPM.Contact'), really := true);
```

```
┌─────────────────┬─────────┬─────────┬───────┐
│    pst_path     │ node_id │ status  │ error │
├─────────────────┼─────────┼─────────┼───────┤
│ /tmp/doc/hr.pst │ 2097380 │ DELETED │ NULL  │
│ /tmp/doc/hr.pst │ 2097348 │ DELETED │ NULL  │
└─────────────────┴─────────┴─────────┴───────┘
```

The arity and types are checked when the statement is bound. `SELECT *` inside a delete is refused: it would materialize every column, bodies and attachment blobs included. Node IDs are checked too, so a folder ID handed to `delete_pst_messages` comes back `FAILED` rather than taking the folder.

### Wiping free space

`wipe_pst_free_space` overwrites every byte the store does not reference. A delete already zeroes the blocks it frees, so this one is for what earlier writers left behind: space Outlook or another client released without clearing, which is where a scrub otherwise leaves readable text.

```sql
select * from wipe_pst_free_space('/tmp/doc/hr.pst', really := true);
```

```
┌─────────────────┬─────────────┬─────────┬───────┐
│    pst_path     │ bytes_wiped │ status  │ error │
├─────────────────┼─────────────┼─────────┼───────┤
│ /tmp/doc/hr.pst │ 1934592     │ DELETED │ NULL  │
└─────────────────┴─────────────┴─────────┴───────┘
```

`bytes_wiped` counts free bytes overwritten, not bytes that changed, so a second wipe on an unchanged store reports the same total.

### Errors

`status` is `PREVIEWED`, `DELETED` or `FAILED`, and `error` carries the reason.

Errors found at bind time abort the statement: the gate being off, or a wrong column count or type. Everything found later is reported per row. A `UNION ALL` input runs as several pipelines that DuckDB finalizes separately, so an earlier pipeline may already have committed deletes by the time a bad row turns up, and throwing would take the record of them with it.

```sql
select status, count(*) from delete_pst_messages(
  (select pst_path, node_id from read_pst_messages('archive/*.pst')
    where body ilike '%SSN%'), really := true)
group by status;
```

A store that reports corruption or a write error abandons the rest of that file's targets. Other files in the same call continue.

Run a delete as its own statement. Deleting from a file the same query reads in another branch is unsupported and undetectable, and `LIMIT 0` on the outer query can skip the finalize pass entirely, deleting nothing.

A read binds its own view of the store, so re-`EXECUTE` of a statement prepared before a delete returns pre-delete results until it is prepared again.

**Note:** paths carrying a scheme other than `file://` are refused. Object storage has no in place edit: writing the file back stores a new version, and the original, which still holds the data you were trying to destroy, stays in the bucket. Copy the file to local disk, delete from it there, and upload the result.

## Schemas

All table functions return PST metadata fields. Message-based functions inherit base `IPM.Note` fields plus type-specific additions.

**Table of Contents:**
- [Common PST Metadata](#common-pst-metadata-all-functions) - Fields present in all table functions
- [Folders](#folders-read_pst_folders) - Folder-specific fields
- [Base Messages](#base-messages-read_pst_messages-read_pst_notes) - Core message fields (emails, notes)
- [Contacts](#contacts-read_pst_contacts) - Contact-specific fields
- [Distribution Lists](#distribution-lists-read_pst_distribution_lists) - Distribution list fields
- [Appointments](#appointments-read_pst_appointments) - Calendar/meeting fields
- [Sticky Notes](#sticky-notes-read_pst_sticky_notes) - Sticky note fields
- [Tasks](#tasks-read_pst_tasks) - Task management fields
- [Struct Schemas](#struct-schemas) - Schemas for recipients, attachments, and one-off members
- [Virtual Columns](#virtual-columns) - PST internal metadata for filtering and optimization

### Common PST Metadata (all functions)

| Field             | Type        | Description                        |
|-------------------|-------------|------------------------------------|
| `pst_path`        | `VARCHAR`   | Full path to the PST file          |
| `pst_name`        | `VARCHAR`   | PST filename without extension     |
| `record_key`      | `BLOB`      | Unique record identifier           |
| `node_id`         | `UINTEGER`  | Node ID within PST                 |
| `parent_node_id`  | `UINTEGER`  | Parent node ID                     |

[↑ Back to Schemas](#schemas)

### Folders (`read_pst_folders`)

Includes PST metadata plus:

| Field                   | Type        | Description                  |
|-------------------------|-------------|------------------------------|
| `container_class`       | `VARCHAR`   | Folder container class       |
| `display_name`          | `VARCHAR`   | Folder display name          |
| `subfolder_count`       | `UINTEGER`  | Number of subfolders         |
| `message_count`         | `BIGINT`    | Total messages in folder     |
| `unread_message_count`  | `BIGINT`    | Unread message count         |

[↑ Back to Schemas](#schemas)

### Base Messages (`read_pst_messages`, `read_pst_notes`)

Includes PST metadata plus:

| Field                    | Type            | Description                                                        |
|--------------------------|-----------------|--------------------------------------------------------------------|
| `subject`                | `VARCHAR`       | Message subject                                                    |
| `body`                   | `VARCHAR`       | Plain text body                                                    |
| `body_html`              | `VARCHAR`       | HTML body                                                          |
| `display_name`           | `VARCHAR`       | Display name                                                       |
| `comment`                | `VARCHAR`       | Comment field                                                      |
| `sender_name`            | `VARCHAR`       | Sender display name                                                |
| `sender_email_address`   | `VARCHAR`       | Sender email                                                       |
| `recipients`             | `LIST(STRUCT)`  | List of [recipients](#recipient-struct)                            |
| `has_attachments`        | `BOOLEAN`       | Whether message has attachments                                    |
| `attachment_count`       | `UINTEGER`      | Number of attachments                                              |
| `attachments`            | `LIST(STRUCT)`  | List of [attachments](#attachment-struct)                          |
| `importance`             | `ENUM`          | Message importance: `LOW`, `NORMAL`, `HIGH`                        |
| `priority`               | `ENUM`          | Message priority: `NONURGENT`, `NORMAL`, `URGENT`                  |
| `sensitivity`            | `ENUM`          | Message sensitivity: `NONE`, `PERSONAL`, `PRIVATE`, `CONFIDENTIAL` |
| `creation_time`          | `TIMESTAMP_S`   | Creation timestamp                                                 |
| `last_modified`          | `TIMESTAMP_S`   | Last modification timestamp                                        |
| `message_delivery_time`  | `TIMESTAMP_S`   | Delivery timestamp                                                 |
| `message_class`          | `VARCHAR`       | MAPI message class (e.g., `IPM.Note`, `IPM.Contact`)               |
| `message_flags`          | `INTEGER`       | Message flags                                                      |
| `message_size`           | `UBIGINT`       | Message size in bytes                                              |
| `conversation_topic`     | `VARCHAR`       | Conversation topic                                                 |
| `internet_message_id`    | `VARCHAR`       | Internet message ID                                                |

[↑ Back to Schemas](#schemas)

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

[↑ Back to Schemas](#schemas)

### Distribution Lists (`read_pst_distribution_lists`)

Includes PST metadata + base message fields + distribution list-specific fields:

| Field               | Type              | Description                                                    |
|---------------------|-------------------|----------------------------------------------------------------|
| `member_node_ids`   | `LIST(UINTEGER)`  | Node IDs of contact members within the PST                     |
| `one_off_members`   | `LIST(STRUCT)`    | List of ([one-off member struct](#one-off-member-struct))      |

[↑ Back to Schemas](#schemas)

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

[↑ Back to Schemas](#schemas)

### Sticky Notes (`read_pst_sticky_notes`)

Includes PST metadata + base message fields + sticky note-specific fields:

| Field         | Type       | Description      |
|---------------|------------|------------------|
| `note_color`  | `INTEGER`  | Note color code  |
| `note_width`  | `INTEGER`  | Note width       |
| `note_height` | `INTEGER`  | Note height      |
| `note_x`      | `INTEGER`  | X position       |
| `note_y`      | `INTEGER`  | Y position       |

[↑ Back to Schemas](#schemas)

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

[↑ Back to Schemas](#schemas)

### Struct Schemas

The following struct types are used in list fields throughout the message schemas:

#### Recipient Struct

Used in the `recipients` field. Each recipient contains:

| Field               | Type       | Description                                                          |
|---------------------|------------|----------------------------------------------------------------------|
| `display_name`      | `VARCHAR`  | Display name of the recipient                                        |
| `account_name`      | `VARCHAR`  | Account name                                                         |
| `email_address`     | `VARCHAR`  | Email address                                                        |
| `address_type`      | `VARCHAR`  | Address type (e.g., "SMTP", "EX")                                    |
| `recipient_type`    | `ENUM`     | Recipient type: `TO`, `CC`, `BCC`                                    |
| `recipient_type_raw`| `INTEGER`  | Raw recipient type value                                             |

[↑ Back to Schemas](#schemas)

#### Attachment Struct

Used in the `attachments` field. Each attachment contains:

| Field                | Type       | Description                                                          |
|----------------------|------------|----------------------------------------------------------------------|
| `node_id`            | `UINTEGER` | Attachment subnode ID, taken by `delete_pst_attachments`             |
| `filename`           | `VARCHAR`  | Attachment filename                                                  |
| `mime_type`          | `VARCHAR`  | MIME type of the attachment                                          |
| `size`               | `UBIGINT`  | Attachment size in bytes                                             |
| `attach_content_id`  | `VARCHAR`  | Content ID for inline attachments                                    |
| `attach_method`      | `ENUM`     | Attachment method: `NO_ATTACHMENT`, `BY_VALUE`, `BY_REFERENCE`, `BY_REF_RESOLVE`, `BY_REF_ONLY`, `EMBEDDED_MESSAGE`, `OLE` |
| `is_message`         | `BOOLEAN`  | Whether attachment is an embedded message                            |
| `bytes`              | `BLOB`     | Raw attachment data                                                  |

[↑ Back to Schemas](#schemas)

#### One-Off Member Struct

Used in the `one_off_members` field of distribution lists. Each one-off member contains:

| Field            | Type       | Description                                              |
|------------------|------------|----------------------------------------------------------|
| `display_name`   | `VARCHAR`  | Display name (e.g., "John Doe (john@example.com)")       |
| `address_type`   | `VARCHAR`  | Address type (typically "SMTP")                          |
| `email_address`  | `VARCHAR`  | Email address                                            |

[↑ Back to Schemas](#schemas)

### Virtual Columns

Virtual columns provide access to PST internal metadata without materializing them in the default projection. These columns are available in all table functions and are primarily used by the late materialization optimizer to cull scans. However, if you know the specific NIDs of one or more objects within a PST, you can use `__node_id` in a filter predicate to push down an efficient scan for only those objects.

| Field              | Type       | Description                                                               |
|--------------------|------------|---------------------------------------------------------------------------|
| `__partition`      | `UBIGINT`  | Nondeterministic internal partition number assigned during query planning |
| `__node_id`        | `UINTEGER` | Node ID within the PST (duplicate of the `node_id` metadata field)        |

[↑ Back to Schemas](#schemas)

## Common Queries

These queries represent common use-cases for traversing PST files or doing basic e-discovery.

**Table of Contents:**
- [Find messages matching a conversation topic](#find-messages-matching-a-conversation-topic)
- [Select directory tree from a given folder](#select-directory-tree-from-a-given-folder)
- [Find all parent directories of a given folder](#find-all-parent-directories-of-a-given-folder)

##### Find messages matching a conversation topic

This pushes down a late-materialized scan against a large directory of PST files:

```sql
select pst_name, conversation_topic, message_size, creation_time 
from read_pst_messages('enron/*.pst')
where conversation_topic LIKE '%EnronOnline%' order by creation_time asc limit 10;

┌────────────────┬────────────────────────────────────────────────┬──────────────┬─────────────────────┐
│    pst_name    │               conversation_topic               │ message_size │    creation_time    │
│    varchar     │                    varchar                     │    uint64    │     timestamp_s     │
├────────────────┼────────────────────────────────────────────────┼──────────────┼─────────────────────┤
│ skilling-j_000 │ EnronOnline Executive Summary for May 15, 2001 │        26953 │ 2010-06-17 04:39:00 │
│ skilling-j_000 │ EnronOnline - Dec 14 Mgt Report                │        10255 │ 2010-06-17 04:39:08 │
│ skilling-j_000 │ EnronOnline - Dec 14 Mgt Report                │        10253 │ 2010-06-17 04:39:08 │
│ rogers-b_000   │ EnronOnline Desk to Desk ID and Password       │         7266 │ 2010-06-17 04:39:23 │
│ rogers-b_000   │ EnronOnline Desk to Desk ID and Password       │         8750 │ 2010-06-17 04:39:26 │
│ jones-t_000    │ EnronOnline Discussion                         │        17672 │ 2010-06-17 04:39:32 │
│ beck-s_000     │ Storing of data on EnronOnline                 │         6833 │ 2010-06-17 04:39:41 │
│ jones-t_000    │ Design Agency for EnronOnline                  │         7442 │ 2010-06-17 04:40:16 │
│ jones-t_000    │ Design Agency for EnronOnline                  │        11830 │ 2010-06-17 04:40:17 │
│ jones-t_000    │ EnronOnline and Offline NDA Lists              │       387401 │ 2010-06-17 04:41:21 │
├────────────────┴────────────────────────────────────────────────┴──────────────┴─────────────────────┤
│ 10 rows                                                                                    4 columns │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
Run Time (s): real 12.695 user 22.185424 sys 7.577354
```

To trigger late materialization, a `LIMIT` clause must generally be present in your query. Limit, Top N, and sample plans are the only ones eligible for LM pushdown. To see whether or not a query will push down a late-materialized scan, use `EXPLAIN` and look for a `HASH_JOIN` operator with `READ_PST_*` scan children.
```
┌─────────────┴─────────────┐
│         HASH_JOIN         │
│    ────────────────────   │
│      Join Type: SEMI      │
│                           │
│        Conditions:        ├──────────────┐
│   __node_id = __node_id   │              │
│ __partition = __partition │              │
│                           │              │
│          ~0 rows          │              │
└─────────────┬─────────────┘              │
```

[↑ Back to Common Queries](#common-queries)

##### Select directory tree from a given folder

This recursively crawls all children of folder with NID 32802.

```sql
with recursive dirtree as (
  select display_name, node_id, parent_node_id
  from read_pst_folders('test/unittest.pst')
  where node_id = 32802
  union
  select f.display_name, f.node_id, f.parent_node_id
  from read_pst_folders('test/unittest.pst') f  
  inner join dirtree d on d.node_id = f.parent_node_id
)

select * from dirtree;

┌──────────────────────────────┬─────────┬────────────────┐
│         display_name         │ node_id │ parent_node_id │
│           varchar            │ uint32  │     uint32     │
├──────────────────────────────┼─────────┼────────────────┤
│ Top of Outlook data file     │   32802 │            290 │
│ Deleted Items                │   32866 │          32802 │
│ Calendar                     │   32994 │          32802 │
│ Sent Items                   │   32962 │          32802 │
│ Outbox                       │   32930 │          32802 │
│ Inbox                        │   32898 │          32802 │
│ Quick Step Settings          │   33250 │          32802 │
│ Conversation Action Settings │   33218 │          32802 │
│ RSS Feeds                    │   33186 │          32802 │
│ Drafts                       │   33154 │          32802 │
│ Tasks                        │   33122 │          32802 │
│ Notes                        │   33090 │          32802 │
│ Journal                      │   33058 │          32802 │
│ Contacts                     │   33026 │          32802 │
├──────────────────────────────┴─────────┴────────────────┤
│ 14 rows                                       3 columns │
└─────────────────────────────────────────────────────────┘
```

[↑ Back to Common Queries](#common-queries)

##### Find all parent directories of a given folder

This traverses the directory tree upward, to the root of the PST file.

```sql
with recursive parent_tree as (
  select display_name, node_id, parent_node_id
  from read_pst_folders('test/unittest.pst')
  where node_id = 33058
  union
  select f.display_name, f.node_id, f.parent_node_id
  from read_pst_folders('test/unittest.pst') f
  inner join parent_tree d on d.parent_node_id = f.node_id
)

select * from parent_tree;

┌──────────────────────────┬─────────┬────────────────┐
│       display_name       │ node_id │ parent_node_id │
│         varchar          │ uint32  │     uint32     │
├──────────────────────────┼─────────┼────────────────┤
│ Journal                  │   33058 │          32802 │
│ Top of Outlook data file │   32802 │            290 │
│                          │     290 │            290 │
└──────────────────────────┴─────────┴────────────────┘
```

[↑ Back to Common Queries](#common-queries)

## Building

```bash
git submodule update --init --recursive
# GEN=ninja make release
# Build with UI extension
GEN=ninja make debug
./build/debug/duckdb -ui
```

###### Windows

```bat
cmake -G "Ninja" -S duckdb -B build ^
 -DFORCE_COLORED_OUTPUT=1 ^
 -DEXTENSION_STATIC_BUILD=1 ^
 -DDUCKDB_EXTENSION_CONFIGS='C:/path/to/duckdb-pst/extension_config.cmake' ^
 -DCMAKE_CXX_STANDARD=17 ^
 -DVCPKG_BUILD=1 ^
 -DCMAKE_TOOLCHAIN_FILE='C:/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake' ^
 -DVCPKG_MANIFEST_DIR='C:/path/to/duckdb-pst/' ^
 -DCMAKE_BUILD_TYPE=Release
cd build
Ninja
```

## Credits

Built with love, by [Intellekt](https://intellekt.fyi).

This extension is built on top of the [microsoft-pst-sdk](https://github.com/enrondata/microsoft-pst-sdk) by [Terry Mahaffey](https://github.com/terrymah), Microsoft's official C++ SDK for reading PST files.
