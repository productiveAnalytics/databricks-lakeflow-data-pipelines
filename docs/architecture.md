# Architecture

## 1. Source simulation

`src/data/customer_cdc.jsonl` is a deterministic CDC feed. The bootstrap job writes it to a Unity Catalog Volume so the Lakeflow Bronze layer consumes a cloud-object-style streaming source through Auto Loader.

The sample contains:

- Inserts for C001-C004
- Updates for C001-C003
- Deletes for C002 and C004
- C001 update sequence 1005 intentionally appears in the file before sequence 1003 to demonstrate sequence-based CDC ordering

## 2. Bronze

Bronze is append-oriented and preserves the CDC envelope:

```text
customer_id
first_name
last_name
email
city
state
operation
sequence_num
```

No SCD logic is applied here.

## 3. Silver

Silver uses Lakeflow `AUTO CDC`:

```text
KEYS(customer_id)
SEQUENCE BY sequence_num
APPLY AS DELETE WHEN operation = 'DELETE'
```

SCD1 materializes current state only. SCD2 creates version history with `__START_AT` and `__END_AT`.

## 4. Gold

Gold intentionally hides implementation mechanics. Both SCD1 and SCD2 expose a `customer_current` dataset with the same consumer-facing columns.

SCD2 Gold applies:

```sql
WHERE __END_AT IS NULL
```

This makes current-state analytics independent of the underlying SCD technique.

## 5. DQX

DQX is executed as a validation stage after each pipeline refresh. It loads YAML rules from the repository, validates the rule definitions, checks the table, writes valid data, quarantines invalid records, and persists summary metrics.

This keeps validation independent from Lakeflow expectations and allows the same rules engine to validate data produced by both the SQL and PySpark implementations.
