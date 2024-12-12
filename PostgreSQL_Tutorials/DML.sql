-- DML: Data Manipulation Language
--- DML is a subset of SQL that is used to manipulate data within a database. It includes commands like INSERT, UPDATE, and DELETE, which allow you to add, modify, or remove data from tables.

--------------------------------------------

-- UPDATE 
UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition;

---- Example 1:
        UPDATE employees 
        SET group = CASE 
                        WHEN salary > 10000 THEN 'Low'
                        WHEN salary > 20000 THEN 'Normal'
                        WHEN salary > 30000 THEN 'High'
                    END 
        WHERE 1=1 ;

---- Example 2:
        WITH cte AS (
            SELECT warehouse_id, product_id
            FROM inventory
            WHERE order_date = '2024-11-01'
        )
        UPDATE fact_sales SET warehouse_id = cte.warehouse_id
        FROM cte
        WHERE fact_sales.product_id = cte.product_id;

--------------------------------------------

-- DELETE
DELETE FROM table_name
WHERE condition;

---- Example 1:
        DELETE FROM employees
        WHERE salary < (SELECT AVG(salary) FROM employees);

-- RETURNING
--- when used with DELETE, it captures and displays the deleted rows (id, name, position, salary) for review.

---- Example 2:
        DELETE FROM employees
        WHERE position IN ('Intern', 'Temporary Data Analyst')
        RETURNING id, name, position, salary;

--------------------------------------------

-- TRUNCATE
--- used to quickly remove all rows from a table. 
--- It is faster than DELETE because it doesn't log individual row deletions or check foreign key constraints (unless explicitly instructed).
TRUNCATE [TABLE] table_name [, ...]
[RESTART IDENTITY | CONTINUE IDENTITY]
[CASCADE | RESTRICT];
/*
1. Removes All Rows:
        TRUNCATE removes all data in the table, but the table structure remains intact.
        It's equivalent to DELETE without a WHERE clause but more efficient.

2. Restarting Sequence Values:
        RESTART IDENTITY: Resets any associated SERIAL or SEQUENCE values to their starting point.
        CONTINUE IDENTITY: Retains the current sequence values.

3. Foreign Key Constraints:
        CASCADE: Automatically truncates related tables that have foreign key references.
        RESTRICT: Prevents truncation if there are dependent foreign key constraints.
*/

-- TRUNCATE vs DELETE
/*
Feature	            TRUNCATE	                DELETE
Speed	            Faster (minimal logging)	Slower (logs each row deletion)
Row-Level Checks	Skips row-level constraints	Checks constraints per row
Foreign Keys	    Skips unless CASCADE used	Always enforced
Returning Clause	Not supported	            Supported
Restart Sequences	Optional (RESTART IDENTITY)	No automatic restart
*/

--------------------------------------------

-- INSERT
INSERT INTO table_name (column1, column2, column3, ...)
VALUES (value1, value2, value3, ...);

INSERT INTO table_name (column1, column2, column3, ...)
SELECT column1, column2, column3, ...
FROM raw_table
WHERE column1 = ...
;

---- Example 1:
        INSERT INTO employees (name, position, salary, hire_date)
        SELECT name, position, salary, CURRENT_DATE
        FROM contractors
        WHERE project = 'Software Development';

--------------------------------------------

-- UPSERT
---  using the INSERT statement with the ON CONFLICT clause. 
--- Allows to insert a new row or update an existing row if a conflict occurs (e.g., a duplicate primary key or unique constraint).
INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON CONFLICT (constraint_column)
DO UPDATE SET column1 = value1, column2 = value2, ...;

/*
    ON CONFLICT: Specifies the column or constraint to check for conflicts.
    DO UPDATE: Defines how to update the existing row when a conflict occurs.
    DO NOTHING: Ignores the conflict and does not insert or update anything.
*/

INSERT INTO transactions (
    transaction_id, customer_id, product_id, quantity, price, total, discount, transaction_date, status, region
)
SELECT 
    transaction_id, 
    customer_id, 
    product_id, 
    quantity, 
    price, 
    transaction_date, 
    status, 
    region
FROM staging_transactions
WHERE transaction_date BETWEEN '2024-01-01 00:00:00' AND '2024-12-31 23:59:59'
ON CONFLICT (transaction_id)
DO UPDATE SET 
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    quantity = EXCLUDED.quantity,
    price = EXCLUDED.price,
    transaction_date = EXCLUDED.transaction_date,
    status = EXCLUDED.status,
    region = EXCLUDED.region;

--------------------------------------------

-- MERGE INTO
--- provides a similar functionality using the INSERT ... ON CONFLICT syntax for upserts.
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET target.column1 = source.column1, target.column2 = source.column2
WHEN NOT MATCHED THEN
    INSERT (column1, column2) VALUES (source.column1, source.column2);

/*
    USING: Specifies the source table for the merge operation.
    ON: Defines the condition for matching rows between the target and source tables.
    WHEN MATCHED THEN: Specifies the action to take when a match is found.
    WHEN NOT MATCHED THEN: Specifies the action to take when no match is found.
*/

---- Example 1:
        MERGE INTO customers AS target
        USING new_customers AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                target.name = source.name,
                target.email = source.email,
                target.phone = source.phone,
                target.join_date = source.join_date
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, phone, join_date)
            VALUES (source.customer_id, source.name, source.email, source.phone, source.join_date);