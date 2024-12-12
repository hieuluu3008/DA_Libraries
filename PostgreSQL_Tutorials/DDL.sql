-- DDL: Data Definition Language
--- DDL is a subset of SQL that is used to define and manage the structure of a database. It includes commands like CREATE, ALTER, and DROP, which allow you to create, modify, or delete database objects such as tables, indexes, and constraints.

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