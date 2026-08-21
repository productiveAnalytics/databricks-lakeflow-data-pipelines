# DQX rule sets

Every implementation has one YAML file per medallion layer. The same quality intent is reused across SQL and PySpark so that implementation differences do not change the data contract.

## Bronze rules

- required customer key
- valid CDC operation
- required sequence number
- unique customer + sequence combination
- email-format warning

## Silver rules

SCD1 adds current-key uniqueness. 
SCD2 adds version start, valid version windows and unique customer-version boundaries.

## Gold rules

- required customer key
- unique current customer
- email format
- nonblank customer name warning
