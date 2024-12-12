-- TCL: Transaction Control Language
--- TCL is a subset of SQL that is used to control the transactional behavior of a database. It includes commands like COMMIT, ROLLBACK, and SAVEPOINT, which allow you to manage and control the state of a transaction.

/* 
TRANSACTION
A transaction is a logical unit of work that consists of one or more SQL statements. 
Transactions are typically used for making significant changes to the database safely.

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