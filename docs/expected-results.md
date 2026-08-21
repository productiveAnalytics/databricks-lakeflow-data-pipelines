# Expected results

All four implementations consume the same 10-event feed in `src/data/customer_cdc.jsonl`
and must therefore produce identical Gold output. Any divergence between the SQL and
PySpark variants is a bug.

## Source feed

| customer_id | operation | sequence_num | city |
|---|---|---:|---|
| C001 | INSERT | 1000 | Chicago |
| C002 | INSERT | 1001 | Austin |
| C003 | INSERT | 1002 | Denver |
| C001 | UPDATE | 1005 | Houston |
| C002 | UPDATE | 1004 | Dallas |
| C001 | UPDATE | 1003 | Dallas |
| C003 | UPDATE | 1006 | Boulder |
| C002 | DELETE | 1010 | — |
| C004 | INSERT | 1011 | Seattle |
| C004 | DELETE | 1012 | — |

The C001 rows appear in the file as 1000, 1005, 1003 — file order deliberately
disagrees with `sequence_num` so the run proves ordering comes from `SEQUENCE BY`,
not from arrival order.

## Bronze (all four implementations)

10 rows, one per source event, with the CDC envelope (`operation`, `sequence_num`)
preserved. No SCD logic.

## SCD Type 1 — Silver and Gold

`silver__type_*__*.customer_dim_scd1` holds current state only, and Gold is a
straight projection of it:

| customer_id | first_name | last_name | city | state |
|---|---|---|---|---|
| C001 | Alice | Nguyen | Houston | TX |
| C003 | Carol | Diaz | Boulder | CO |

C002 and C004 are absent: their highest-sequence event is a DELETE, and
`APPLY AS DELETE WHEN operation = 'DELETE'` physically removes the row under SCD Type 1.

If C001 shows `Dallas` instead of `Houston`, sequencing was not applied — that is the
failure this data set is designed to catch.

## SCD Type 2 — Silver

`silver__type_*__*.customer_dim_scd2` holds every version. Because `SEQUENCE BY` is the
BIGINT `sequence_num`, `__START_AT` and `__END_AT` are BIGINT sequence values, not
timestamps. A row is current when `__END_AT IS NULL`.

| customer_id | city | state | __START_AT | __END_AT |
|---|---|---|---:|---:|
| C001 | Chicago | IL | 1000 | 1003 |
| C001 | Dallas | TX | 1003 | 1005 |
| C001 | Houston | TX | 1005 | NULL |
| C002 | Austin | TX | 1001 | 1004 |
| C002 | Dallas | TX | 1004 | 1010 |
| C003 | Denver | CO | 1002 | 1006 |
| C003 | Boulder | CO | 1006 | NULL |
| C004 | Seattle | WA | 1011 | 1012 |

8 rows total.

A DELETE under SCD Type 2 does **not** insert a tombstone row. It closes the open
version by setting `__END_AT` to the delete event's sequence number, which is why C002
and C004 have no row with a NULL `__END_AT`. The `city`/`state` NULLs carried on the
DELETE events never reach the table.

## SCD Type 2 — Gold

`WHERE __END_AT IS NULL` reduces the history to the same two rows the SCD Type 1
Gold table contains:

| customer_id | first_name | last_name | city | state |
|---|---|---|---|---|
| C001 | Alice | Nguyen | Houston | TX |
| C003 | Carol | Diaz | Boulder | CO |

## DQX

The sample feed is clean by construction, so a correct run quarantines nothing:

| Table | `__dq_output` rows | `__dq_quarantine` rows |
|---|---:|---:|
| Bronze (any implementation) | 10 | 0 |
| Silver SCD1 | 2 | 0 |
| Silver SCD2 | 8 | 0 |
| Gold (any implementation) | 2 | 0 |

A non-empty quarantine table means either the pipeline produced something unexpected or
a rule in `dqx/checks/` drifted from the data contract. `<table>__dq_metrics` is written
in `append` mode, so it accumulates one row set per DQX job run.

## Why this test is useful

The C001 records prove CDC ordering comes from `sequence_num` rather than file arrival
order. The C002 and C004 deletes exercise both current-state deletion (Type 1) and
version closing (Type 2). Because both SCD types converge on the same Gold contract,
the two Gold tables can be diffed directly as a regression check.
