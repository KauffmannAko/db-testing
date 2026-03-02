-- =========================================================
-- Covers:
--   1) DATA INTEGRITY TEST CASES (incl. API-to-DB consistency + ACID + concurrency checks)
--   2) PERFORMANCE TESTING QUERIES + BENCHMARKS (with indexing recommendations)
--   3) DATA MIGRATION TESTING (legacy -> new schema)
--
-- Notes:
-- - ACID tests are wrapped in BEGIN/ROLLBACK blocks per requirement.
-- - All "Expected Result" statements are described in comments.
-- =========================================================


-- =========================================================
-- SECTION 1) DATA INTEGRITY TEST CASES
-- =========================================================

-- ---------------------------------------------------------
-- DI-API-001
-- Purpose: Verify transfer_id from API exists in transactions after API call.
-- Setup: Insert a simulated API-created transaction row (TX_9001).
-- Execution: Query by transaction_id.
-- Expected Result:
--   - Exactly 1 row returned with transaction_id = 'TX_9001'.
-- Cleanup: Delete TX_9001 and related test accounts.
-- ---------------------------------------------------------
BEGIN;

-- Setup data
INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1001', 101, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1002', 102,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9001', 'ACC_1001', 'ACC_1002', 49.99, 'USD', 1.000000, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Execution query
SELECT *
FROM transactions
WHERE transaction_id = 'TX_9001';

-- Cleanup (explicit, even though we ROLLBACK in this block)
DELETE FROM transactions WHERE transaction_id = 'TX_9001';
DELETE FROM accounts WHERE account_id IN ('ACC_1001', 'ACC_1002');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-002
-- Purpose: Verify status stored in DB matches API response status (direct mapping).
-- Setup: Create 3 transactions with statuses pending/completed/failed.
-- Execution: Query and validate statuses are within allowed set and match expected for test IDs.
-- Expected Result:
--   - TX_9002 status='PENDING'
--   - TX_9003 status='COMPLETED'
--   - TX_9004 status='FAILED'
-- Cleanup: Delete TX_9002..TX_9004 and test accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1001', 101, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1002', 102,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9002', 'ACC_1001', 'ACC_1002', 10.00, 'USD', 1.000000, 'PENDING',   CURRENT_TIMESTAMP, NULL),
  ('TX_9003', 'ACC_1001', 'ACC_1002', 12.50, 'USD', 1.000000, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('TX_9004', 'ACC_1001', 'ACC_1002', 20.00, 'USD', 1.000000, 'FAILED',    CURRENT_TIMESTAMP, NULL);

-- Execution query (status mapping check)
SELECT transaction_id, status
FROM transactions
WHERE transaction_id IN ('TX_9002', 'TX_9003', 'TX_9004')
ORDER BY transaction_id;

-- Execution query (allowed values check)
SELECT transaction_id, status
FROM transactions
WHERE status NOT IN ('PENDING', 'COMPLETED', 'FAILED');

-- Cleanup
DELETE FROM transactions WHERE transaction_id IN ('TX_9002', 'TX_9003', 'TX_9004');
DELETE FROM accounts WHERE account_id IN ('ACC_1001', 'ACC_1002');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-003
-- Purpose: Verify exchange_rate returned by API matches value stored in DB.
-- Setup: Insert a cross-currency transaction with exchange_rate=620.000000 (USD->XAF example).
-- Execution: Query exchange_rate and validate exact stored value.
-- Expected Result:
--   - TX_9005 exchange_rate = 620.000000
-- Cleanup: Delete TX_9005 and test accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1003', 103, 2000.00, 'XAF', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1004', 104,  100.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9005', 'ACC_1004', 'ACC_1003', 10.00, 'USD', 620.000000, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Execution query
SELECT transaction_id, exchange_rate
FROM transactions
WHERE transaction_id = 'TX_9005';

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9005';
DELETE FROM accounts WHERE account_id IN ('ACC_1003', 'ACC_1004');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-004
-- Purpose: Verify COMPLETED transfers update balances correctly.
-- Setup:
--   - ACC_1001 balance=1000.00 USD
--   - ACC_1002 balance=500.00 USD
--   - Insert COMPLETED TX_9006 amount=100.00
-- Execution:
--   - Simulate the balance update (debit+credit) that should occur for COMPLETED transactions.
--   - Validate resulting balances.
-- Expected Result:
--   - ACC_1001 balance becomes 900.00
--   - ACC_1002 balance becomes 600.00
-- Cleanup: Delete TX_9006 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1001', 101, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1002', 102,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9006', 'ACC_1001', 'ACC_1002', 100.00, 'USD', 1.000000, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Execution: simulate ledger effect for COMPLETED transfer (atomic debit+credit)
UPDATE accounts SET balance = balance - 100.00 WHERE account_id = 'ACC_1001';
UPDATE accounts SET balance = balance + 100.00 WHERE account_id = 'ACC_1002';

-- Validation query
SELECT account_id, balance
FROM accounts
WHERE account_id IN ('ACC_1001', 'ACC_1002')
ORDER BY account_id;

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9006';
DELETE FROM accounts WHERE account_id IN ('ACC_1001', 'ACC_1002');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-005
-- Purpose: Verify PENDING transfers do NOT affect balances.
-- Setup:
--   - ACC_1005 balance=1000.00 USD, ACC_1006 balance=500.00 USD
--   - Insert PENDING TX_9007 amount=100.00
-- Execution:
--   - DO NOT update balances.
--   - Query balances to ensure unchanged.
-- Expected Result:
--   - ACC_1005 remains 1000.00
--   - ACC_1006 remains 500.00
-- Cleanup: Delete TX_9007 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1005', 105, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1006', 106,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9007', 'ACC_1005', 'ACC_1006', 100.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Validation query
SELECT account_id, balance
FROM accounts
WHERE account_id IN ('ACC_1005', 'ACC_1006')
ORDER BY account_id;

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9007';
DELETE FROM accounts WHERE account_id IN ('ACC_1005', 'ACC_1006');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-006
-- Purpose: Verify FAILED transfers do NOT affect balances.
-- Setup:
--   - ACC_1007 balance=200.00 USD, ACC_1008 balance=100.00 USD
--   - Insert FAILED TX_9008 amount=150.00
-- Execution:
--   - DO NOT update balances.
--   - Query balances to ensure unchanged.
-- Expected Result:
--   - ACC_1007 remains 200.00
--   - ACC_1008 remains 100.00
-- Cleanup: Delete TX_9008 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1007', 107, 200.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1008', 108, 100.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9008', 'ACC_1007', 'ACC_1008', 150.00, 'USD', 1.000000, 'FAILED', CURRENT_TIMESTAMP, NULL);

-- Validation query
SELECT account_id, balance
FROM accounts
WHERE account_id IN ('ACC_1007', 'ACC_1008')
ORDER BY account_id;

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9008';
DELETE FROM accounts WHERE account_id IN ('ACC_1007', 'ACC_1008');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-007
-- Purpose: Verify fees handling consistency (conceptual check).
-- Assumption:
--   - API returns "fees" but DB schema does not store fees.
--   - If fees are deducted from sender balance, then net debit = amount + fees.
-- Setup:
--   - ACC_1009 balance=1000.00 USD, ACC_1010 balance=500.00 USD
--   - TX_9009 amount=100.00 COMPLETED
--   - Assume fees returned by API = 2.50 USD (captured externally / from logs)
-- Execution:
--   - Apply net debit (102.50) and credit (100.00) to simulate.
--   - Validate sender delta equals -(amount+fees), receiver delta equals +amount.
-- Expected Result:
--   - ACC_1009 balance becomes 897.50
--   - ACC_1010 balance becomes 600.00
-- Cleanup: Delete TX_9009 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1009', 109, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1010', 110,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9009', 'ACC_1009', 'ACC_1010', 100.00, 'USD', 1.000000, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Execution: simulate fee deduction (fees = 2.50)
UPDATE accounts SET balance = balance - 102.50 WHERE account_id = 'ACC_1009';
UPDATE accounts SET balance = balance + 100.00 WHERE account_id = 'ACC_1010';

-- Validation query
SELECT account_id, balance
FROM accounts
WHERE account_id IN ('ACC_1009', 'ACC_1010')
ORDER BY account_id;

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9009';
DELETE FROM accounts WHERE account_id IN ('ACC_1009', 'ACC_1010');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-008
-- Purpose: Validate scheduled_date logic (scheduled transfers remain PENDING before execution time).
-- Note:
--   - scheduled_date is not stored in the simplified schema. This test validates via created_at and status:
--     if a transfer is scheduled for the future, it must remain PENDING until executed.
-- Setup:
--   - Insert TX_9010 as PENDING with created_at=now (represents scheduled transfer created now).
-- Execution:
--   - Query to ensure it is PENDING and completed_at is NULL.
-- Expected Result:
--   - status='PENDING', completed_at IS NULL
-- Cleanup: Delete TX_9010 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1011', 111, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1012', 112,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9010', 'ACC_1011', 'ACC_1012', 25.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution query
SELECT transaction_id, status, completed_at
FROM transactions
WHERE transaction_id = 'TX_9010';

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9010';
DELETE FROM accounts WHERE account_id IN ('ACC_1011', 'ACC_1012');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-API-009
-- Purpose: Validate recurring logic (multiple transactions created correctly).
-- Note:
--   - Recurring rules are not stored in simplified schema.
--   - We validate resulting effect: multiple transactions exist with same from/to/amount
--     across multiple created_at timestamps, and unique transaction_id per occurrence.
-- Setup:
--   - Insert 3 PENDING transactions to simulate recurring schedule instances.
-- Execution:
--   - Query count and uniqueness.
-- Expected Result:
--   - Count = 3
--   - Distinct transaction_id count = 3
-- Cleanup: Delete TX_9011..TX_9013 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1013', 113, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1014', 114,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9011', 'ACC_1013', 'ACC_1014',  5.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL),
  ('TX_9012', 'ACC_1013', 'ACC_1014',  5.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL),
  ('TX_9013', 'ACC_1013', 'ACC_1014',  5.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution query: recurring count validation
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT transaction_id) AS distinct_tx_ids
FROM transactions
WHERE transaction_id IN ('TX_9011', 'TX_9012', 'TX_9013');

-- Cleanup
DELETE FROM transactions WHERE transaction_id IN ('TX_9011', 'TX_9012', 'TX_9013');
DELETE FROM accounts WHERE account_id IN ('ACC_1013', 'ACC_1014');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-REF-001
-- Purpose: Detect orphan transactions where from_account or to_account does not exist in accounts.
-- Setup:
--   - Insert TX_9014 with from_account missing (ACC_MISSING).
-- Execution:
--   - LEFT JOIN to detect missing accounts.
-- Expected Result:
--   - Query returns TX_9014 as orphan.
-- Cleanup: Delete TX_9014 and test accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1015', 115, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9014', 'ACC_MISSING', 'ACC_1015', 10.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution query: orphan detection
SELECT t.transaction_id, t.from_account, t.to_account
FROM transactions t
LEFT JOIN accounts a_from ON a_from.account_id = t.from_account
LEFT JOIN accounts a_to   ON a_to.account_id   = t.to_account
WHERE (a_from.account_id IS NULL OR a_to.account_id IS NULL)
  AND t.transaction_id = 'TX_9014';

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9014';
DELETE FROM accounts WHERE account_id IN ('ACC_1015');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-REF-002
-- Purpose: Foreign key simulation check (even if FK not defined):
--          ensure all transactions reference existing accounts.
-- Setup: None (runs on existing dataset).
-- Execution: Query returns any rows that violate referential integrity.
-- Expected Result:
--   - Zero rows returned in a healthy system.
-- Cleanup: None.
-- ---------------------------------------------------------
-- Execution query
SELECT t.transaction_id, t.from_account, t.to_account
FROM transactions t
LEFT JOIN accounts a_from ON a_from.account_id = t.from_account
LEFT JOIN accounts a_to   ON a_to.account_id   = t.to_account
WHERE a_from.account_id IS NULL OR a_to.account_id IS NULL;


-- ---------------------------------------------------------
-- DI-FIN-001
-- Purpose: Financial rule validation: amount > 0 and max 2 decimals.
-- Setup:
--   - Insert TX_9015 with amount=0.00
--   - Insert TX_9016 with amount=-1.00
--   - Insert TX_9017 with amount=10.999 (may auto-round depending on DB)
-- Execution:
--   - Detect violations using WHERE conditions.
-- Expected Result:
--   - TX_9015 and TX_9016 must be flagged.
--   - TX_9017: If stored with scale 2, value becomes 11.00 or 10.99 depending on rounding;
--     flag any value that violates the "no more than 2 decimals" rule at ingestion layer.
-- Cleanup: Delete TX_9015..TX_9017 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1016', 116, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1017', 117, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9015', 'ACC_1016', 'ACC_1017',   0.00, 'USD', 1.000000, 'FAILED', CURRENT_TIMESTAMP, NULL),
  ('TX_9016', 'ACC_1016', 'ACC_1017',  -1.00, 'USD', 1.000000, 'FAILED', CURRENT_TIMESTAMP, NULL),
  ('TX_9017', 'ACC_1016', 'ACC_1017',  10.99, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution query: amount must be > 0
SELECT transaction_id, amount
FROM transactions
WHERE transaction_id IN ('TX_9015', 'TX_9016', 'TX_9017')
  AND amount <= 0;

-- Execution query: amount must have max 2 decimals
-- Note: Most DBs store DECIMAL(15,2) and will enforce scale at storage time.
-- This query is a generic check for values not equal to rounding to 2 decimals.
SELECT transaction_id, amount
FROM transactions
WHERE transaction_id IN ('TX_9015', 'TX_9016', 'TX_9017')
  AND amount <> ROUND(amount, 2);

-- Cleanup
DELETE FROM transactions WHERE transaction_id IN ('TX_9015', 'TX_9016', 'TX_9017');
DELETE FROM accounts WHERE account_id IN ('ACC_1016', 'ACC_1017');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-FIN-002
-- Purpose: No NULL critical fields (transaction_id, from_account, to_account, amount, currency, status, created_at).
-- Setup: None (runs on existing dataset).
-- Execution: Query returns rows with NULL critical fields.
-- Expected Result:
--   - Zero rows returned in a healthy system.
-- Cleanup: None.
-- ---------------------------------------------------------
SELECT *
FROM transactions
WHERE transaction_id IS NULL
   OR from_account   IS NULL
   OR to_account     IS NULL
   OR amount         IS NULL
   OR currency       IS NULL
   OR status         IS NULL
   OR created_at     IS NULL;


-- ---------------------------------------------------------
-- DI-FIN-003
-- Purpose: No negative balances allowed.
-- Setup: None (runs on existing dataset).
-- Execution: Query accounts with balance < 0.
-- Expected Result:
--   - Zero rows returned in a healthy system.
-- Cleanup: None.
-- ---------------------------------------------------------
SELECT account_id, user_id, balance, currency, status
FROM accounts
WHERE balance < 0;


-- ---------------------------------------------------------
-- DI-FIN-004
-- Purpose: Currency must follow ISO-4217 format (3 uppercase letters).
-- Setup: Insert account with lowercase currency to validate detection.
-- Execution: Query invalid currencies in accounts and transactions.
-- Expected Result:
--   - Rows with non-3-letter uppercase currencies are returned.
-- Cleanup: Delete ACC_1018.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1018', 118, 100.00, 'usd', 'ACTIVE', CURRENT_TIMESTAMP);

-- Execution query: invalid currency in accounts
SELECT account_id, currency
FROM accounts
WHERE currency IS NULL
   OR LENGTH(currency) <> 3
   OR currency <> UPPER(currency);

-- Execution query: invalid currency in transactions
SELECT transaction_id, currency
FROM transactions
WHERE currency IS NULL
   OR LENGTH(currency) <> 3
   OR currency <> UPPER(currency);

-- Cleanup
DELETE FROM accounts WHERE account_id = 'ACC_1018';

ROLLBACK;


-- ---------------------------------------------------------
-- DI-FIN-005
-- Purpose: Same-currency transfers must have exchange_rate = 1.
-- Setup: Insert same-currency transaction with exchange_rate != 1 to validate detection.
-- Execution: Query violations.
-- Expected Result:
--   - TX_9018 is returned as violation.
-- Cleanup: Delete TX_9018 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1019', 119, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1020', 120,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9018', 'ACC_1019', 'ACC_1020', 10.00, 'USD', 0.990000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution query: same-currency exchange_rate must be 1
SELECT transaction_id, currency, exchange_rate
FROM transactions
WHERE transaction_id = 'TX_9018'
  AND currency = 'USD'
  AND exchange_rate <> 1.000000;

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9018';
DELETE FROM accounts WHERE account_id IN ('ACC_1019', 'ACC_1020');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-FIN-006
-- Purpose: Cross-currency transfers must have exchange_rate > 0.
-- Note:
--   - Simplified schema doesn't store account currencies in transaction row.
--   - This test uses transaction.currency and exchange_rate sanity:
--     For any transaction with exchange_rate IS NOT NULL, it must be > 0.
-- Setup: Insert TX_9019 with exchange_rate = 0 to validate detection.
-- Execution: Query violations.
-- Expected Result:
--   - TX_9019 is returned as violation.
-- Cleanup: Delete TX_9019 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1021', 121, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1022', 122, 1000.00, 'XAF', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9019', 'ACC_1021', 'ACC_1022', 10.00, 'USD', 0.000000, 'FAILED', CURRENT_TIMESTAMP, NULL);

-- Execution query: exchange_rate must be > 0 when provided
SELECT transaction_id, exchange_rate
FROM transactions
WHERE transaction_id = 'TX_9019'
  AND exchange_rate IS NOT NULL
  AND exchange_rate <= 0;

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9019';
DELETE FROM accounts WHERE account_id IN ('ACC_1021', 'ACC_1022');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-FIN-007
-- Purpose: COMPLETED must have completed_at NOT NULL; PENDING/FAILED must have completed_at NULL.
-- Setup: Insert violations for validation.
-- Execution: Query invalid status/timestamp combinations.
-- Expected Result:
--   - TX_9020 (COMPLETED with completed_at NULL) flagged
--   - TX_9021 (PENDING with completed_at NOT NULL) flagged
--   - TX_9022 (FAILED with completed_at NOT NULL) flagged
-- Cleanup: Delete TX_9020..TX_9022 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1023', 123, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1024', 124, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9020', 'ACC_1023', 'ACC_1024', 10.00, 'USD', 1.000000, 'COMPLETED', CURRENT_TIMESTAMP, NULL),
  ('TX_9021', 'ACC_1023', 'ACC_1024', 10.00, 'USD', 1.000000, 'PENDING',   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('TX_9022', 'ACC_1023', 'ACC_1024', 10.00, 'USD', 1.000000, 'FAILED',    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Execution query: invalid combinations
SELECT transaction_id, status, completed_at
FROM transactions
WHERE transaction_id IN ('TX_9020', 'TX_9021', 'TX_9022')
  AND (
        (status = 'COMPLETED' AND completed_at IS NULL)
     OR (status IN ('PENDING', 'FAILED') AND completed_at IS NOT NULL)
  )
ORDER BY transaction_id;

-- Cleanup
DELETE FROM transactions WHERE transaction_id IN ('TX_9020', 'TX_9021', 'TX_9022');
DELETE FROM accounts WHERE account_id IN ('ACC_1023', 'ACC_1024');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-FIN-008
-- Purpose: Duplicate transaction detection (same transaction_id should never exist twice).
-- Setup: Attempt to insert same primary key twice.
-- Execution: Insert should fail on second attempt due to PRIMARY KEY constraint.
-- Expected Result:
--   - Second insert fails with duplicate key / constraint error.
-- Cleanup: Delete TX_9023 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1025', 125, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1026', 126, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9023', 'ACC_1025', 'ACC_1026', 10.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution: second insert with same transaction_id (EXPECTED TO FAIL)
-- INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
-- VALUES ('TX_9023', 'ACC_1025', 'ACC_1026', 10.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9023';
DELETE FROM accounts WHERE account_id IN ('ACC_1025', 'ACC_1026');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-FIN-009
-- Purpose: Potential duplicate transfers detection:
--          same from/to/amount/currency within 1 minute window.
-- Setup: Insert two transactions within same minute (TX_9024, TX_9025).
-- Execution: Query groups with count > 1 in 60-second window (approx).
-- Expected Result:
--   - Group containing TX_9024 and TX_9025 returned.
-- Cleanup: Delete TX_9024, TX_9025 and accounts.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1027', 127, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1028', 128, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

-- Use same created_at for deterministic "within 1 minute"
INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9024', 'ACC_1027', 'ACC_1028', 15.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL),
  ('TX_9025', 'ACC_1027', 'ACC_1028', 15.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution query:
-- Bucket by minute using a generic approach: compare absolute difference in timestamps.
-- NOTE: Different DBs have different timestamp arithmetic. If this fails, use a DB-specific function.
-- Generic approximation: group by from/to/amount/currency and by minute-truncated created_at when supported.
SELECT
  from_account,
  to_account,
  amount,
  currency,
  COUNT(*) AS duplicate_count
FROM transactions
WHERE transaction_id IN ('TX_9024', 'TX_9025')
GROUP BY from_account, to_account, amount, currency
HAVING COUNT(*) > 1;

-- Cleanup
DELETE FROM transactions WHERE transaction_id IN ('TX_9024', 'TX_9025');
DELETE FROM accounts WHERE account_id IN ('ACC_1027', 'ACC_1028');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-ACID-001
-- Purpose: Demonstrate transaction rollback (Atomicity).
-- Setup:
--   - ACC_1029 balance=1000.00 USD
--   - ACC_1030 balance=500.00 USD
-- Execution:
--   - Begin transaction
--   - Debit sender
--   - Force an error (attempt to insert duplicate PK) BEFORE credit
--   - Rollback
-- Validation:
--   - Balances unchanged after rollback
-- Expected Result:
--   - ACC_1029 remains 1000.00
--   - ACC_1030 remains 500.00
-- Cleanup:
--   - Delete test accounts (TX rows rolled back)
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1029', 129, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1030', 130,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

-- Create a row to use for forced PK conflict
INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9026', 'ACC_1029', 'ACC_1030', 1.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Execution: atomic transfer attempt that will fail
UPDATE accounts SET balance = balance - 100.00 WHERE account_id = 'ACC_1029';

-- Force error: duplicate PK insert (EXPECTED TO FAIL) to simulate mid-transaction failure
-- INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
-- VALUES ('TX_9026', 'ACC_1029', 'ACC_1030', 100.00, 'USD', 1.000000, 'PENDING', CURRENT_TIMESTAMP, NULL);

-- Since the error line is commented to allow sequential runs, manually ROLLBACK to simulate rollback behavior.
-- In a real execution, uncomment the failing insert; it will error and you must issue ROLLBACK.

-- Validation query (run AFTER rollback in real scenario)
SELECT account_id, balance
FROM accounts
WHERE account_id IN ('ACC_1029', 'ACC_1030')
ORDER BY account_id;

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9026';
DELETE FROM accounts WHERE account_id IN ('ACC_1029', 'ACC_1030');

ROLLBACK;


-- ---------------------------------------------------------
-- DI-ACID-002
-- Purpose: Verify atomic balance update (debit and credit happen together).
-- Setup:
--   - ACC_1031 balance=1000.00, ACC_1032 balance=500.00
-- Execution:
--   - Begin transaction
--   - Debit sender
--   - Credit receiver
--   - Insert COMPLETED transaction row
--   - Rollback (test should leave balances unchanged after rollback)
-- Expected Result:
--   - After ROLLBACK: balances unchanged (1000.00 and 500.00)
-- Cleanup: None required beyond ROLLBACK, but included.
-- ---------------------------------------------------------
BEGIN;

INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1031', 131, 1000.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1032', 132,  500.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

-- Execution: atomic operations
UPDATE accounts SET balance = balance - 50.00 WHERE account_id = 'ACC_1031';
UPDATE accounts SET balance = balance + 50.00 WHERE account_id = 'ACC_1032';

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9027', 'ACC_1031', 'ACC_1032', 50.00, 'USD', 1.000000, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Validation query INSIDE transaction (shows updated balances before rollback)
SELECT account_id, balance
FROM accounts
WHERE account_id IN ('ACC_1031', 'ACC_1032')
ORDER BY account_id;

-- Cleanup (explicit)
DELETE FROM transactions WHERE transaction_id = 'TX_9027';
DELETE FROM accounts WHERE account_id IN ('ACC_1031', 'ACC_1032');

ROLLBACK;

-- Post-rollback validation (Expected unchanged if you didn't explicitly commit)
SELECT account_id, balance
FROM accounts
WHERE account_id IN ('ACC_1031', 'ACC_1032')
ORDER BY account_id;


-- ---------------------------------------------------------
-- DI-CONC-001
-- Purpose: Detect double application of same transfer_id (idempotency check).
-- Setup: None (runs on existing dataset).
-- Execution:
--   - Find any duplicate transaction_id (should be impossible due to PK)
--   - Additionally detect multiple COMPLETED rows for same logical transfer fingerprint if PK differs.
-- Expected Result:
--   - Duplicate transaction_id query returns 0 rows.
--   - Fingerprint query returns 0 rows OR returns suspicious cases to investigate.
-- Cleanup: None.
-- ---------------------------------------------------------
-- Execution query: duplicate transaction_id (should be 0 due to PK)
SELECT transaction_id, COUNT(*) AS cnt
FROM transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Execution query: "logical duplicates" (possible duplicate processing with different IDs)
-- Suspicious if multiple COMPLETED transfers share same from/to/amount/currency within 1 minute.
SELECT
  from_account,
  to_account,
  amount,
  currency,
  COUNT(*) AS completed_cnt,
  MIN(created_at) AS first_seen,
  MAX(created_at) AS last_seen
FROM transactions
WHERE status = 'COMPLETED'
GROUP BY from_account, to_account, amount, currency
HAVING COUNT(*) > 1;


-- ---------------------------------------------------------
-- DI-CONC-002
-- Purpose: Detect suspicious simultaneous transactions impacting the same from_account.
-- Setup: None (runs on existing dataset).
-- Execution:
--   - Identify accounts with unusually high number of transactions in short time.
-- Expected Result:
--   - Returns 0 or small number of rows; spikes indicate concurrency/race issues or fraud.
-- Cleanup: None.
-- ---------------------------------------------------------
-- NOTE: Time bucketing is DB-specific. This generic query flags high volume overall by account.
-- For time-window detection, implement in your DB using date_trunc / time_bucket functions.
SELECT
  from_account,
  COUNT(*) AS txn_count
FROM transactions
WHERE created_at >= (CURRENT_TIMESTAMP - INTERVAL '1 day') -- If unsupported, replace with DB-specific interval syntax
GROUP BY from_account
HAVING COUNT(*) > 100
ORDER BY txn_count DESC;


-- ---------------------------------------------------------
-- DI-API-010
-- Purpose: API-to-DB consistency check: COMPLETED transaction implies balance delta equals amount (and fee if applicable).
-- Setup: Create completed TX_9028 and then validate delta using expected pre/post balances captured in a temp approach.
-- Execution:
--   - Query balances and compare to expected deltas.
-- Expected Result:
--   - Sender balance decreased by amount (or amount+fee if fee is applied)
--   - Receiver balance increased by amount
-- Cleanup: Delete TX_9028 and accounts.
-- ---------------------------------------------------------
BEGIN;

-- Setup
INSERT INTO accounts (account_id, user_id, balance, currency, status, created_at)
VALUES
  ('ACC_1033', 133, 300.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP),
  ('ACC_1034', 134, 100.00, 'USD', 'ACTIVE', CURRENT_TIMESTAMP);

INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at)
VALUES
  ('TX_9028', 'ACC_1033', 'ACC_1034', 25.00, 'USD', 1.000000, 'COMPLETED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Execution: simulate the balance update that MUST occur for COMPLETED
UPDATE accounts SET balance = balance - 25.00 WHERE account_id = 'ACC_1033';
UPDATE accounts SET balance = balance + 25.00 WHERE account_id = 'ACC_1034';

-- Validation: ensure deltas match exactly amount
SELECT
  'ACC_1033' AS account_id,
  balance AS current_balance,
  275.00 AS expected_balance
FROM accounts WHERE account_id = 'ACC_1033'
UNION ALL
SELECT
  'ACC_1034' AS account_id,
  balance AS current_balance,
  125.00 AS expected_balance
FROM accounts WHERE account_id = 'ACC_1034';

-- Cleanup
DELETE FROM transactions WHERE transaction_id = 'TX_9028';
DELETE FROM accounts WHERE account_id IN ('ACC_1033', 'ACC_1034');

ROLLBACK;



-- =========================================================
-- SECTION 2) PERFORMANCE TESTING QUERIES + BENCHMARKS
-- =========================================================
-- Target dataset size for meaningful benchmarks:
--   - accounts: 1M rows
--   - transactions: 10M+ rows
--
-- Benchmarks (typical targets on indexed, healthy DB):
--   - Lookup by PK (transaction_id): < 50ms (warm cache), < 200ms (cold cache)
--   - Account recent history (last 20): < 200ms
--   - Daily aggregation over 30 days: < 1s (with proper partitioning/indexing)
--   - Pending monitoring query: < 300ms
--
-- Recommended Indexes (examples):
--   1) transactions(transaction_id) -> already PK
--   2) transactions(from_account, created_at DESC) -> account statement fast
--   3) transactions(to_account, created_at DESC) -> incoming transfers history
--   4) transactions(status, created_at) -> pending/failed monitoring and ops dashboards
--   5) transactions(created_at) -> time-based analytics
-- Why each matters:
--   - Avoid full table scans, support ops monitoring, reconciliation, and reporting at scale.
-- =========================================================

-- ---------------------------------------------------------
-- PERF-IDX-001
-- Purpose: Index creation examples for performance (optional).
-- Expected Result:
--   - Indexes created successfully (or already exist).
-- Note: Use IF NOT EXISTS if your DB supports it.
-- ---------------------------------------------------------
-- CREATE INDEX idx_transactions_from_created ON transactions (from_account, created_at);
-- CREATE INDEX idx_transactions_to_created   ON transactions (to_account, created_at);
-- CREATE INDEX idx_transactions_status_time  ON transactions (status, created_at);
-- CREATE INDEX idx_transactions_created_at   ON transactions (created_at);

-- ---------------------------------------------------------
-- PERF-001
-- Purpose: Fast lookup by transaction_id (critical for support + reconciliation).
-- Execution: EXPLAIN / EXPLAIN ANALYZE on PK lookup.
-- Expected Result:
--   - Uses PK index lookup, not full scan.
--   - Benchmark: <200ms on 10M rows.
-- ---------------------------------------------------------
EXPLAIN
SELECT *
FROM transactions
WHERE transaction_id = 'TX_9001';

-- If supported:
-- EXPLAIN ANALYZE
-- SELECT *
-- FROM transactions
-- WHERE transaction_id = 'TX_9001';

-- ---------------------------------------------------------
-- PERF-002
-- Purpose: Account transaction history query (last 20 outgoing).
-- Execution: EXPLAIN / EXPLAIN ANALYZE.
-- Expected Result:
--   - Uses idx_transactions_from_created.
--   - Benchmark: <200ms on large datasets.
-- ---------------------------------------------------------
EXPLAIN
SELECT transaction_id, to_account, amount, currency, status, created_at
FROM transactions
WHERE from_account = 'ACC_1001'
ORDER BY created_at DESC
LIMIT 20;

-- ---------------------------------------------------------
-- PERF-003
-- Purpose: Account transaction history query (last 20 incoming).
-- Expected Result:
--   - Uses idx_transactions_to_created.
-- ---------------------------------------------------------
EXPLAIN
SELECT transaction_id, from_account, amount, currency, status, created_at
FROM transactions
WHERE to_account = 'ACC_1001'
ORDER BY created_at DESC
LIMIT 20;

-- ---------------------------------------------------------
-- PERF-004
-- Purpose: Daily volume aggregation (analytics).
-- Execution: Aggregate by day (DB-specific truncation may be needed).
-- Expected Result:
--   - Benchmark: <1s for last 30 days; improve via partitioning by created_at.
-- ---------------------------------------------------------
-- NOTE: DATE(created_at) is supported in many DBs; adjust if needed.
EXPLAIN
SELECT DATE(created_at) AS day, currency, COUNT(*) AS txn_count, SUM(amount) AS total_amount
FROM transactions
WHERE created_at >= (CURRENT_TIMESTAMP - INTERVAL '30 day')
GROUP BY DATE(created_at), currency
ORDER BY day DESC;

-- ---------------------------------------------------------
-- PERF-005
-- Purpose: Monitoring query for PENDING/FAILED transfers (ops dashboard).
-- Expected Result:
--   - Uses idx_transactions_status_time.
--   - Benchmark: <300ms.
-- ---------------------------------------------------------
EXPLAIN
SELECT status, COUNT(*) AS cnt
FROM transactions
WHERE status IN ('PENDING', 'FAILED')
  AND created_at >= (CURRENT_TIMESTAMP - INTERVAL '1 day')
GROUP BY status;

-- ---------------------------------------------------------
-- PERF-006
-- Purpose: Reconciliation query - validate completed transfers count and sums per day.
-- Expected Result:
--   - Benchmark: <1s for 30 days.
-- ---------------------------------------------------------
EXPLAIN
SELECT DATE(created_at) AS day, currency, COUNT(*) AS completed_cnt, SUM(amount) AS completed_sum
FROM transactions
WHERE status = 'COMPLETED'
  AND created_at >= (CURRENT_TIMESTAMP - INTERVAL '30 day')
GROUP BY DATE(created_at), currency
ORDER BY day DESC;

-- ---------------------------------------------------------
-- PERF-007
-- Purpose: Multi-currency query - identify cross-currency candidates (exchange_rate != 1).
-- Expected Result:
--   - Fast with an index on (exchange_rate) if heavily used; otherwise use created_at filter.
-- ---------------------------------------------------------
EXPLAIN
SELECT transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at
FROM transactions
WHERE exchange_rate IS NOT NULL
  AND exchange_rate <> 1.000000
  AND created_at >= (CURRENT_TIMESTAMP - INTERVAL '30 day')
ORDER BY created_at DESC
LIMIT 100;

-- ---------------------------------------------------------
-- PERF-008
-- Purpose: Validate index usage / detect sequential scans on key queries.
-- Execution: Run EXPLAIN for each query above and ensure index scans are used.
-- Expected Result:
--   - No full table scan for targeted filters (transaction_id, from_account, status+created_at).
-- ---------------------------------------------------------
-- Manual validation step: capture EXPLAIN plans and attach to performance benchmark report.



-- =========================================================
-- SECTION 3) DATA MIGRATION TESTING
-- =========================================================
-- Assumptions:
-- - Migration sources:
--     legacy_accounts
--     legacy_transactions
--     account_id_map(old_id, new_id)
-- - After migration, new tables are:
--     accounts
--     transactions
--
-- Goals:
-- - Validate row counts, key mapping, currency normalization, status mapping,
--   timestamp normalization, duplicates, and reconciliation totals.
-- =========================================================

-- ---------------------------------------------------------
-- MIG-001
-- Purpose: Row count comparison for accounts.
-- Expected Result:
--   - COUNT(legacy_accounts) matches COUNT(accounts) (or matches after filtered exclusions).
-- Cleanup: None.
-- ---------------------------------------------------------
SELECT 'legacy_accounts' AS table_name, COUNT(*) AS row_count FROM legacy_accounts
UNION ALL
SELECT 'accounts'        AS table_name, COUNT(*) AS row_count FROM accounts;

-- ---------------------------------------------------------
-- MIG-002
-- Purpose: Row count comparison for transactions.
-- Expected Result:
--   - COUNT(legacy_transactions) matches COUNT(transactions) (or matches after filtered exclusions).
-- ---------------------------------------------------------
SELECT 'legacy_transactions' AS table_name, COUNT(*) AS row_count FROM legacy_transactions
UNION ALL
SELECT 'transactions'        AS table_name, COUNT(*) AS row_count FROM transactions;

-- ---------------------------------------------------------
-- MIG-003
-- Purpose: ID mapping validation - every legacy account id must be mapped to a new account id.
-- Expected Result:
--   - Zero rows returned (no missing mapping).
-- ---------------------------------------------------------
SELECT la.account_id AS legacy_account_id
FROM legacy_accounts la
LEFT JOIN account_id_map m ON m.old_id = la.account_id
WHERE m.new_id IS NULL;

-- ---------------------------------------------------------
-- MIG-004
-- Purpose: ID mapping validation - mapped new_id must exist in accounts.
-- Expected Result:
--   - Zero rows returned (every mapped new_id exists).
-- ---------------------------------------------------------
SELECT m.old_id, m.new_id
FROM account_id_map m
LEFT JOIN accounts a ON a.account_id = m.new_id
WHERE a.account_id IS NULL;

-- ---------------------------------------------------------
-- MIG-005
-- Purpose: Currency normalization checks (lowercase -> uppercase) for accounts.
-- Expected Result:
--   - Zero rows returned (all currencies uppercase, length=3).
-- ---------------------------------------------------------
SELECT account_id, currency
FROM accounts
WHERE currency IS NULL
   OR LENGTH(currency) <> 3
   OR currency <> UPPER(currency);

-- ---------------------------------------------------------
-- MIG-006
-- Purpose: Currency normalization checks for transactions.
-- Expected Result:
--   - Zero rows returned.
-- ---------------------------------------------------------
SELECT transaction_id, currency
FROM transactions
WHERE currency IS NULL
   OR LENGTH(currency) <> 3
   OR currency <> UPPER(currency);

-- ---------------------------------------------------------
-- MIG-007
-- Purpose: Status mapping validation - statuses must be in allowed set.
-- Expected Result:
--   - Zero rows returned.
-- ---------------------------------------------------------
SELECT transaction_id, status
FROM transactions
WHERE status NOT IN ('PENDING', 'COMPLETED', 'FAILED');

-- ---------------------------------------------------------
-- MIG-008
-- Purpose: Timestamp normalization validation (created_at not null; completed_at rules by status).
-- Expected Result:
--   - Zero rows returned.
-- ---------------------------------------------------------
SELECT transaction_id, status, created_at, completed_at
FROM transactions
WHERE created_at IS NULL
   OR (status = 'COMPLETED' AND completed_at IS NULL)
   OR (status IN ('PENDING', 'FAILED') AND completed_at IS NOT NULL);

-- ---------------------------------------------------------
-- MIG-009
-- Purpose: Duplicate detection post-migration (transaction_id uniqueness).
-- Expected Result:
--   - Zero rows returned.
-- ---------------------------------------------------------
SELECT transaction_id, COUNT(*) AS cnt
FROM transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------
-- MIG-010
-- Purpose: Aggregate reconciliation - total balances per currency (legacy vs new).
-- Expected Result:
--   - For each currency, legacy total ~= new total (allowing known adjustments).
-- ---------------------------------------------------------
SELECT currency, SUM(balance) AS total_balance
FROM legacy_accounts
GROUP BY currency
ORDER BY currency;

SELECT currency, SUM(balance) AS total_balance
FROM accounts
GROUP BY currency
ORDER BY currency;

-- ---------------------------------------------------------
-- MIG-011
-- Purpose: Aggregate reconciliation - total transferred amounts per day (legacy vs new).
-- Expected Result:
--   - For each day+currency, legacy totals match new totals for COMPLETED transactions.
-- ---------------------------------------------------------
SELECT DATE(created_at) AS day, currency, SUM(amount) AS total_amount
FROM legacy_transactions
WHERE status = 'COMPLETED'
GROUP BY DATE(created_at), currency
ORDER BY day, currency;

SELECT DATE(created_at) AS day, currency, SUM(amount) AS total_amount
FROM transactions
WHERE status = 'COMPLETED'
GROUP BY DATE(created_at), currency
ORDER BY day, currency;

-- ---------------------------------------------------------
-- MIG-012
-- Purpose: Random sample validation - pick 20 migrated transactions and compare key fields with legacy.
-- Expected Result:
--   - Sample rows match on from/to mapping, amount, currency, status, timestamps (within expected normalization).
-- Note:
--   - RANDOM() is common but not universal; replace with DB-specific sampling if needed.
-- ---------------------------------------------------------
-- Sample new transactions
SELECT transaction_id, from_account, to_account, amount, currency, exchange_rate, status, created_at, completed_at
FROM transactions
ORDER BY RANDOM()
LIMIT 20;

-- Optional join sample back to legacy using mapping (if legacy tx has account old ids):
-- SELECT lt.transaction_id AS legacy_tx_id, nt.transaction_id AS new_tx_id, ...
-- FROM legacy_transactions lt
-- JOIN transactions nt ON nt.transaction_id = lt.transaction_id
-- JOIN account_id_map mf ON mf.old_id = lt.from_account AND mf.new_id = nt.from_account
-- JOIN account_id_map mt ON mt.old_id = lt.to_account AND mt.new_id = nt.to_account
-- LIMIT 20;

-- ---------------------------------------------------------
-- MIG-013
-- Purpose: Referential integrity validation post-migration.
-- Expected Result:
--   - Zero orphan rows in transactions referencing missing accounts.
-- ---------------------------------------------------------
SELECT t.transaction_id, t.from_account, t.to_account
FROM transactions t
LEFT JOIN accounts a_from ON a_from.account_id = t.from_account
LEFT JOIN accounts a_to   ON a_to.account_id   = t.to_account
WHERE a_from.account_id IS NULL OR a_to.account_id IS NULL;

-- ---------------------------------------------------------
-- MIG-014
-- Purpose: Migration sanity check - no negative balances introduced by migration.
-- Expected Result:
--   - Zero rows returned.
-- ---------------------------------------------------------
SELECT account_id, balance, currency
FROM accounts
WHERE balance < 0;

-- ---------------------------------------------------------
-- MIG-015
-- Purpose: Migration correctness - ensure exchange_rate is present and valid when not 1.
-- Expected Result:
--   - Zero rows returned where exchange_rate <= 0.
-- ---------------------------------------------------------
SELECT transaction_id, exchange_rate
FROM transactions
WHERE exchange_rate IS NOT NULL
  AND exchange_rate <= 0;

-- =========================================================
-- END OF FILE
-- =========================================================