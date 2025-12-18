# duckdb-pst

A DuckDB extension for efficiently reading data out of [Microsoft PST files](https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-pst/141923d5-15ab-4ef1-a524-6dce75aae546), with rich schemas for common MAPI types. It is fully integrated with DuckDB's filesystem allowing direct reads from local disk or object storage (with globbing). Use it to query PST files using plain SQL, or to import their data into DuckDB tables (or any other supported federated write).

## Features

- Supports MAPI types: `IPM.Note`, `IPM.Appointment`, `IPM.Contact`, `IPM.StickyNote`, `IPM.Task`
- Optimized planning: concurrent partition planning for large dirs & pre-applied MAPI type filters
- Projection pushdown: only materialize requested columns
- Statistics pushdown: fast `count(*)` without full scans
- DuckDB filesystem integration: read PSTs from S3, ABFS, etc
- Progress API for large scans
- Late materialization (sort-of): filter on (virtual columns + minimum projection) before expanding to produce results

## Getting Started

Quickly count all messages or folders in a directory full of PSTs (this one has 167 files):

```sql
memory D select count(*) from read_pst_folders('enron/*.pst');
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│         5305 │
└──────────────┘
Run Time (s): real 0.565 user 0.103431 sys 0.811655

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

Read the first 10 messages (with limit applied during planning -- for large files):

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

## Usage

Call these table functions to read MAPI items and their specific fields. All PST messages have a base class of `IPM.Note`: `read_pst_messages` will read all messages in the archive regardless of their type.

| Table Function            | MAPI Message Class  | Description                                              |
|---------------------------|---------------------|----------------------------------------------------------|
| `read_pst_folders`        | `*`                 | Folders                                                  |
| `read_pst_messages`       | `*`                 | All messages, with base `IPM.Note` email projection      |
| `read_pst_notes`          | `IPM.Note`          | (Filtered) only `IPM.Note` (and unimplemented types)     |
| `read_pst_contacts`       | `IPM.Contact`       | (Filtered) only contacts with contact-specific fields    |
| `read_pst_appointments`   | `IPM.Appointment`   | (Filtered) only calendar appointments and meetings       |
| `read_pst_sticky_notes`   | `IPM.StickyNote`    | (Filtered) only sticky note items                        |
| `read_pst_tasks`          | `IPM.Task`          | (Filtered) task items with task-specific fields          |

## Building

```bash
git submodule update --init --recursive
# GEN=ninja make release
# Build with UI extension
GEN=ninja make debug
./build/debug/duckdb -ui
```
