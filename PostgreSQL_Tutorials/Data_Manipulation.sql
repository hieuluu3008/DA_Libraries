/* 
TRANSACTION
A transaction is a logical unit of work that consists of one or more SQL statements. Transactions are typically used for making significant changes to the database safely.

A transaction begins with BEGIN or START TRANSACTION.
During execution, you can choose to:
Use COMMIT to save all changes made within the transaction.
Use ROLLBACK to undo all changes made within the transaction.
*/

-- COMMIT
--- Saves all changes made in the transaction to the database. Once a COMMIT is executed, the changes become permanent and cannot be undone.

---- Example:
        BEGIN;

        INSERT INTO accounts (name, balance) VALUES ('Alice', 1000);
        UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice';

        COMMIT;

-- ROLLBACK
--- Undoes all changes made in the transaction. Once a ROLLBACK is executed, the changes are discarded and the database returns to its state before the transaction began.

---- Example:
        BEGIN;

        UPDATE accounts SET balance = balance - 200 WHERE name = 'Alice';
        -- Error: Insufficient funds or incorrect information

        ROLLBACK; -- Undo the changes, reverting the database to its original state

-- COMMIT + ROLLBACK
---- Example:
        BEGIN;

        INSERT INTO accounts (name, balance) VALUES ('Bob', 500);

        -- Check for errors
        IF EXISTS (SELECT 1 FROM accounts WHERE name = 'Bob' AND balance < 0) THEN
            ROLLBACK; -- Rollback the transaction if an error occurs
        ELSE
            COMMIT; -- Commit the transaction if successful
        END IF;

-- SAVEPOINT
--- A savepoint is a point within a transaction where you can create a checkpoint. You can use savepoints to:
        --- Rollback to a specific point in the transaction.
        --- Rollback to a savepoint and continue from there.


-- COMMIT + ROLLBACK + SAVEPOINT
---- Example:
        BEGIN;

        SAVEPOINT my_savepoint; -- Create a savepoint named 'my_savepoint'

        UPDATE accounts SET balance = balance - 200 WHERE name = 'Alice';

        -- Check for errors
        IF EXISTS (SELECT 1 FROM accounts WHERE name = 'Alice' AND balance < 0) THEN
            ROLLBACK TO my_savepoint; -- Rollback to the 'my_savepoint' savepoint
        ELSE
            RELEASE my_savepoint; -- Release the 'my_savepoint' savepoint
            COMMIT; -- Commit the transaction if successful
        END IF;

/* 
By default, PostgreSQL operates in autocommit mode, 
meaning each individual SQL statement is treated as a transaction and executed immediately without requiring COMMIT.
*/

--------------------------------------------

-- PROCEDURE
CREATE PROCEDURE procedure_name(parameter_name data_type, ...)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Your SQL code here
END;
$$;

--- Run a procedure:
Call procedure_name(parameter_value, ...);

---- Example:
        CREATE PROCEDURE insert_data(IN name TEXT, IN age INT)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO people (name, age) VALUES (name, age);
        END;
        $$;

        CALL insert_data('John Doe', 30);

--- Drop a procedure:
DROP PROCEDURE procedure_name(parameter_types_if_any);

--- Listing procedures:
SELECT proname AS procedure_name, proargtypes AS argument_types
FROM pg_proc
WHERE prokind = 'p'; -- 'p' denotes procedures

--------------------------------------------

-- FUNCTION
CREATE FUNCTION function_name(parameter_name data_type, ...)
RETURNS return_data_type
LANGUAGE language_name
AS $$
BEGIN
    -- Function body (SQL or procedural code)
    RETURN value;
END;
$$;

---- Example 1:
        CREATE FUNCTION check_even_odd(num INT)
        RETURNS TEXT
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF num % 2 = 0 THEN
                RETURN 'Even';
            ELSE
                RETURN 'Odd';
            END IF;
        END;
        $$;

        -- Usage
        SELECT check_even_odd(5); -- Output: 'Odd'
        SELECT check_even_odd(10); -- Output: 'Even'

---- Example 2:
        CREATE FUNCTION factorial(n INT)
        RETURNS BIGINT
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF n = 0 OR n = 1 THEN
                RETURN 1;
            ELSE
                RETURN n * factorial(n - 1);
            END IF;
        END;
        $$;

        -- Usage
        SELECT factorial(5); -- Output: 120

--- Listing functions:
SELECT proname AS function_name, proargtypes AS argument_types, prorettype AS return_type
FROM pg_proc
WHERE prokind = 'f';

--- Drop function:
DROP FUNCTION function_name(parameter_types);

--------------------------------------------

/*
TRIGGER
A mechanism that automatically executes a function when a specified event occurs on a table or view.
Triggers are commonly used to perform tasks such as validating data, maintaining database integrity, or logging changes.

Trigger Elements:
    Triggering Event: The event that activates the trigger, such as INSERT, UPDATE, DELETE, or TRUNCATE.
    
    Triggering Time: The timing of the trigger execution:
        BEFORE: Executed before the event occurs.
        AFTER: Executed after the event occurs.
        INSTEAD OF: Replaces the event (applicable only to views).
    
    Trigger Function: The function that is executed when the trigger is activated. The trigger function must be created beforehand.
*/

--- Create log_table:
CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    table_name TEXT,
    operation TEXT,
    changed_data JSON,
    change_time TIMESTAMP DEFAULT current_timestamp
);

--- Trigger Function:
CREATE FUNCTION log_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log(table_name, operation, changed_data, change_time)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW), current_timestamp);
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log(table_name, operation, changed_data, change_time)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW), current_timestamp);
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log(table_name, operation, changed_data, change_time)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD), current_timestamp);
    END IF;
    RETURN NULL; -- No need to return anything for AFTER TRIGGER
END;
$$;
/*
TG_OP: Indicates the event type (INSERT, UPDATE, DELETE).
TG_TABLE_NAME: The name of the table triggering the event.
NEW and OLD: Represent the new and old row data in the trigger.
*/

--- Trigger:
CREATE TRIGGER trigger_name
{ BEFORE | AFTER | INSTEAD OF }
{ INSERT | UPDATE | DELETE | TRUNCATE }
ON table_name
FOR EACH { ROW | STATEMENT }
EXECUTE FUNCTION trigger_function_name();

---- Example:
        CREATE TRIGGER audit_log_trigger
        AFTER INSERT OR UPDATE OR DELETE
        ON employees
        FOR EACH ROW
        EXECUTE FUNCTION log_changes();
/*
AFTER INSERT OR UPDATE OR DELETE: Activates after the INSERT, UPDATE, or DELETE events.
FOR EACH ROW: Executes the trigger for every affected row.
*/

--- Delete Trigger:
DROP TRIGGER trigger_name ON table_name;

--- Listing triggers:
SELECT trigger_name, event_object_table AS table_name, action_statement AS trigger_function
FROM information_schema.triggers
WHERE trigger_schema = 'public';

--------------------------------------------

-- VIEW
CREATE VIEW view_name AS
SELECT column1, column2, ...
FROM table_name
WHERE condition;

---- Example:
        CREATE OR REPLACE VIEW active_employees AS
        SELECT id, name, department, status
        FROM employees
        WHERE status = 'active';

        --- Usage:
        SELECT * FROM active_employees;

--- Drop a view:
DROP VIEW view_name;

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

--------------------------------------------