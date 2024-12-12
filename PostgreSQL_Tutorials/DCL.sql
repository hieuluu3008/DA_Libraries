-- DCL: Data Control Language
--- DCL is a subset of SQL that is used to control access to data and database objects. It includes commands like GRANT and REVOKE, which allow you to manage user permissions and roles within the database.

-- GRANT
--- Used to give specific privileges on database objects to users or roles.
    GRANT privilege [, ...] 
    ON object_type object_name 
    TO { user | role | PUBLIC } [, ...] 
    [ WITH GRANT OPTION ];

/*Common Privileges:
SELECT: Allows the user to query data.
INSERT: Allows inserting new data.
UPDATE: Allows modifying existing data.
DELETE: Allows deleting data.
USAGE: Grants usage of schemas or sequences.
ALL PRIVILEGES: Grants all available privileges. */
---- Example:
GRANT SELECT, INSERT ON employees TO john;

-- REVOKE
--- Used to remove specific privileges from users or roles.
    REVOKE privilege [, ...]
    ON object_type object_name
    FROM { user | role | PUBLIC } [, ...]
    [ CASCADE | RESTRICT ];

---- Example:
REVOKE SELECT ON employees FROM john;

--- View detailed privilege info:
SELECT grantee, privilege_type 
FROM information_schema.role_table_grants 
WHERE table_name = 'employees';
