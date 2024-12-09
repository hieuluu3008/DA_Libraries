/*
INDEX
Indexes are essential for optimizing query performance by allowing the database to quickly locate rows without scanning the entire table
Index are often on a column used for filtering (date, location, ...) or primary key 
Avoid use index with columns have null value
Index do take storage space that we will have to pay for
An index is only effective for queries that include a `WHERE` condition on the indexed column.
*/

-- Sử dụng index là việc scan trên 2 table. Tức là scan index table trước, sau đó ánh xạ kết quả sang table chính.
-- Trong một số trường hợp, PostgreSQL sẽ tự đưa ra query execution phù hợp
---- Example 1:
    EXPLAIN ANALYZE SELECT * FROM ENGINEER WHERE COUNTRY_ID >= 100;
    -- Query plan 
        -- Seq Scan on engineer (cost=0.00..2367.00 rows=58427 width=52) (actual time=0.020..820.922 rows=58497 loops=1)
        --   Filter: (country_id >= 100)
        --   Rows removed by Filter: 41503
        -- Planning time: 0.217 ms
        -- Execution time: 1570.828 ms
---- Example 2:
    EXPLAIN ANALYZE SELECT * FROM ENGINEER WHERE COUNTRY_ID >= 150;
    -- Query plan
        -- Bitmap Heap Scan on engineer (cost=421.86..2005.89 rows=37363 width=52) (actual time=3.614..525.345 rows=37363 loops=1)
        --   Recheck Cond: (country_id >= 150)
        --   Heap Blocks: exact=1117
        --   ->  Bitmap Index Scan on idx_engineer_country_id (cost=0.00..412.51 rows=37363 width=52) (actual time=3.315..3.323 rows=37759 loops=1)
        --         Index Cond: (country_id >= 150)
        -- Planning time: 0.202 ms
        -- Execution time: 1011.887 ms
/* 
Lí do country_id >= 100 sử dụng seq scan còn country_id >= 150 vì với ới điều kiện >= 100, có rất nhiều records phù hợp. 
Do đó query execution xác định rằng seq scan trên table chính còn nhanh hơn việc scan index trên table index 
rồi sau đó mất công look-up sang table chính
*/

-- a. B-Tree Index (Balanced Tree Index) (Default)
--     B-Tree index uses the **Binary Tree** data structure for storage, and Binary Search Tree (BST) is employed to perform searches.
--     Best for equality and range queries.
--     Suitable for =, <, >, <=, >=, and BETWEEN.
--     Efficient for large tables with high cardinality.
--     Efficient for columns with sequential or increasing data (e.g., ID, date).
CREATE INDEX index_name ON table_name (column_name);
---- Example 1: Create an index on the "date" column of the "orders" table
CREATE INDEX idx_orders_date ON orders (date);

--- Bitmap Index
/*
Bản chất của bitmap index vẫn sử dụng cấu trúc B-Tree, tuy nhiên nó lưu trữ thông tin khác với B-Tree index. 
B-Tree index mapping index với một hoặc nhiều rowId. 
Bitmap index mapping index với giá trị bit tương ứng của column. 
Ví dụ có 3 giá trị của gender: Male, Female, Unknown. Tạo ra 3 bit tương ứng 0, 1, 2 cho 3 giá trị đó và mapping với column trong table chính.

Tính chất:
    - Phù hợp với các column low cardinality.
    - Lưu bit cho mỗi giá trị nên giảm dung lượng lưu trữ cần dùng.
    - Chỉ hiệu quả với tìm kiếm full match value.
    - Kết hợp với nhiều index khác để tăng tốc độ với OR, AND.
Hạn chế:
    - Nếu thêm hoặc bớt một giá trị cần build lại toàn bộ index table. Với B-Tree index chỉ cần re-balance tree.
    - Với PostgreSQL, bitmap index được lưu trên memory vì size của nó khá nhỏ, từ đó tăng tốc độ truy vấn. 
        Vì vậy khi restart nó cần build lại toàn bộ bitmap index. 
        Để tránh nhược điểm này, trong thực tế sẽ sử dụng kết hợp với column khác tạo thành composite index.
*/


-- b. Hash Index
--     Optimized for equality comparisons (=). (Faster than B-Tree)
--     Not as versatile as B-Tree.
--     Suitable for columns with a small number of distinct values.
--     Not suitable for columns with high cardinality or sequential data.
--     Can't create composite index with hash index
CREATE INDEX index_name ON table_name USING hash (column_name);
---- Example 2: Create a unique index on the "id" column of the "customers" table
CREATE INDEX idx_user_id_hash ON users USING hash (user_id);

-- c. GIN (Generalized Inverted Index)
--     Used for data types with multiple values, such as:
        -- Full-text search.
        -- JSONB (queries on JSON columns).
        -- Arrays.
--     Efficient for indexing multi-valued data.
CREATE INDEX index_name ON table_name USING gin (column_name);
---- Example 3: Create a GIN index on the "description" column of the "products" table
CREATE INDEX idx_content_search ON articles USING gin (to_tsvector('english', content));
CREATE INDEX idx_json_data ON data_table USING gin (json_column);

-- d. GiST (Generalized Search Tree)
--     Used for complex data types such as:
        -- Geometric data (shapes).
        -- Full-text search.
        -- Range queries.
CREATE INDEX index_name ON table_name USING gist (column_name);
---- Example 4: Create a GiST index on the "location" column of the "employees" table
CREATE INDEX idx_employees_location ON employees USING gist (location);

-- e. BRIN (Block Range Index)
--     Efficient for very large tables with sequentially ordered data (e.g., date or increasing ID columns).
--     Stores data ranges instead of individual values.
--     Saves storage compared to B-Tree.
CREATE INDEX index_name ON table_name USING brin (column_name);
---- Example 5: Create a BRIN index on the "id" column of the "orders" table
CREATE INDEX idx_orders_id ON orders USING brin (id);

-- f. Expression Index
--     Creates an index on an expression or function instead of directly on a column.
CREATE INDEX index_name ON table_name (expression);
---- Example 6: Create an expression index on the "total_price" column of the "orders" table
CREATE INDEX idx_orders_total_price ON orders ((quantity * price));

-- g. Partial Index
--     Creates an index on a subset of table data based on a condition (WHERE clause).
CREATE INDEX index_name ON table_name (column_name) WHERE condition;
---- Example 7: Create a partial index on the "status" column of the "orders" table
CREATE INDEX idx_orders_status ON orders (status) WHERE status IN ('pending', 'shipped');

-- h. Unique Index
--     Ensures that all values in the indexed column are unique.
CREATE UNIQUE INDEX index_name ON table_name (column_name);
---- Example 8: Create a unique index on the "email" column of the "customers" table
CREATE UNIQUE INDEX idx_customers_email ON customers (email);

-- Covering Index (INCLUDE)
--     An extension of B-Tree Index, allowing additional columns to be stored for query optimization without indexing them directly.
--     Reducing the index size.
--     Useful when querying a subset of columns.
CREATE INDEX index_name ON table_name (column_name1, column_name2) INCLUDE (column_name3, column_name4);
---- Example 9: Create a covering index on the "name" and "email" columns of the "customers" table
CREATE INDEX idx_customers_name_email ON customers (name, email) INCLUDE (phone);

/*
Comparison of Index Types:
Index Type	    Use Case	                Advantages	                Disadvantages
--------------------------------------------------------------------------------------------------------
B-Tree	        Most queries	            Versatile, widely used	    More resource-intensive than Hash
Hash	        Exact match (=)	            Fast for = queries	        Does not support ranges
GIN	            Full-text, JSON, array	    Efficient for multi-value	Resource-intensive
GiST	        Geometric, range	        Flexible	                More complex configuration
BRIN	        Large sequential tables	    Compact, fast	            Sequential data only
Partial	        Specific conditions (WHERE)	Saves storage	            Narrow application scope
Expression	    Expressions, functions	    Flexible	                Requires careful management
*/

-- Partial Index
/*
A partial index in SQL is an index built on a subset of rows in a table, defined by a specific condition in the form of a WHERE clause. 
It is a useful optimization technique to improve query performance 
and save storage when an index is only necessary for certain rows that meet the specified condition.
*/
CREATE INDEX index_name 
ON table_name (column_name)
WHERE condition;
---- Example:
    CREATE INDEX active_users_idx 
    ON users (last_login)
    WHERE is_active = TRUE;


-- Index Concurrently
/*
The keyword CONCURRENTLY is used when creating or dropping an index to minimize the locking of a table,
allowing other operations (like INSERT, UPDATE, and DELETE) to continue while the index is being created or dropped.
    Slower than normal index creation because of the multiple-step process.
    It uses more resources and may slow down other queries during the index creation process.
    Useful when creating indexes on large tables in production environments where downtime is unacceptable.
*/
CREATE INDEX CONCURRENTLY index_name ON table_name (column_name);
---- Example 10: Create a concurrently index on the "date" column of the "orders" table
CREATE INDEX CONCURRENTLY idx_orders_date ON orders (date);

--- View Indexes: 
        \di table_name -- To list all indexes for a table
        
        SELECT
            indexname, indexdef
        FROM
            pg_indexes
        WHERE
            tablename = 'table_name';
--- Drop Indexes:
        DROP INDEX index_name;

--- Maintain Indexes: (Rebuild, Reindex, Vacuum)
        REINDEX INDEX index_name; / REINDEX TABLE table_name; 
        -- Re-index a column or table
        -- Useful when indexes become bloated or damaged
        VACUUM FULL table_name;
        --Like deep cleaning your house - removes completely dead rows and reclaims disk space
        -- Compacts tables to their minimum size
        -- Warning: Requires exclusive lock on the table, so use during low-traffic periods
        CLUSTER table_name USING index_name;
        -- Physically reorders table data based on an index
        -- Imagine reorganizing a filing cabinet so related documents are stored together
        -- Can improve performance for range-based queries
        ANALYZE table_name;
        VACUUM ANALYZE table_name;
        -- Combines VACUUM (basic cleanup) with ANALYZE
        -- Two-in-one maintenance operation
        -- Removes dead rows and updates statistics in one go

--- Analyzing Indexes:
        EXPLAIN ANALYZE SELECT * FROM table_name WHERE column_name = 'value';

--------------------------------------------

-- EXPLAIN
/*
EXPLAIN command is used to display the execution plan that PostgreSQL generates for a given query. 
This plan shows how the database processes the query, including the steps it takes and the methods it uses to retrieve data
*/
EXPLAIN SELECT * FROM employees WHERE department = 'Sales';
--- Example output:
Seq Scan on employees  (cost=0.00..12.75 rows=5 width=37)
Filter: (department = 'Sales')
--- Explain:
    -- Seq Scan: The query is using a sequential scan to read the employees table. (when table don't have index)
            /*
            Sequential Scan is when the database reads every single row in a table from beginning to end,
            checking each row against the query conditions. 
            It's like reading a book page by page to find specific information, rather than using the index
            */
    -- cost=0.00..12.75: Estimated cost of execution (start cost and total cost).
    -- rows=5: Estimated number of rows to be returned.
    -- width=37: Estimated size of each row (in bytes).
    -- Filter: The condition applied to filter rows.

-- EXPLAIN ANALYZE (explain combine with analyze)
--- Combines EXPLAIN with query execution, showing the actual execution time and detailed statistics.
EXPLAIN ANALYZE SELECT * FROM employees WHERE department = 'Sales';
--- Example output:
Seq Scan on employees  (cost=0.00..12.75 rows=5 width=37) (actual time=0.024..0.026 rows=5 loops=1)
Filter: (department = 'Sales')
Rows Removed by Filter: 10

/*
Analyze option sẽ thực thi các statement chứ không đơn thuần là plan nữa. Vì vậy cần rất cẩn thận khi thêm option này trong quá trình explain. 
Ví dụ với các DML statement (INSERT/DELETE/UPDATE):
    Nếu chỉ thực hiện explain thì không có vấn đề gì xảy ra.
    Nếu thực hiện explain analyze thì.. còn cái nịt 😂.
Do vậy, nếu muốn thực thi các DML statement với explain analyze cần sử dụng với transaction:
*/
BEGIN;
EXPLAIN ANALYZE DELETE FROM engineer;
ROLLBACK;
--- Example output:
Delete on engineer  (cost=0.00..12.75 rows=5 width=37) (actual time=0.024..0.026 rows=5 loops=1)
Planning Time: 0.053 ms
Execution Time: 2418.210 ms
--- Explain:
    -- Actual time: con số đầu tiên thể hiện thời gian cần để khởi động, con số thứ hai là thời gian để hoàn thành.
    -- loops: số lượng vòng lặp
    -- Planning: thời gian lên kế hoạch cho query
    -- execution time: 2418 ms. Vì sao lại có sự chênh lệch giữa execution time và actual time? 
        --- Actual time là thời gian tính toán cho nhiệm vụ seq scan table. còn một nhiệm vụ quan trọng nữa là fetch data để hiển thị.

--- Key Componenents in EXPLAIN Output
/*
Component	       |  Description (Mô tả)
-----------------------------------------
Seq Scan	        Sequential scan: Reads all rows in the table.
Index Scan	        Uses an index to locate matching rows.
Bitmap Index Scan	Efficiently accesses rows in large datasets using a bitmap structure.
Nested Loop	        Joins tables by iterating over one table and finding matches in another.
Hash Join	        Joins tables by building a hash table from one table and matching rows from the other.
Cost	            Estimated effort to execute the query (lower is better).
Rows	            Estimated number of rows to be returned.
Actual Time	        Real execution time for each step (only shown with ANALYZE).
Loops	            Number of times the operation was executed.
*/


--- Tips
    -- Always Use ANALYZE for Real Execution Details
    -- Enable VERBOSE Mode for more info. (Shows additional internal details, such as which schema and columns are involved)
    EXPLAIN (VERBOSE, ANALYZE) SELECT * FROM employees WHERE department = 'Sales';
    -- Combine with SET for Testing Different Plans 
        -- (Helps you experiment with different configurations, such as enabling or disabling certain types of scans (e.g., sequential or index scans))
    SET enable_seqscan = off; -- Disable Sequential Scan
    EXPLAIN SELECT * FROM employees WHERE department = 'Sales';

--------------------------------------------

-- PARTITION
/*
Table partitioning is a database design technique that divides a large table into smaller, 
more manageable pieces known as partitions. Each partition is a sub-table that holds a subset of the data, 
with each row existing in exactly one partition
*/
--- A. Veritcal Partitioning
/*
Vertical partitioning in SQL involves splitting a table into multiple smaller tables based on columns. 
This is done to optimize performance, manageability, and storage efficiency, 
especially when a table has a large number of columns that are not always accessed together.
Each partition typically shares the same primary key to enable joins when needed.
*/

--- B. Horizontal partitioning
/*
 Dividing a table into multiple smaller tables (or partitions) based on rows. 
 Each partition contains a subset of the rows from the original table, 
 often distributed across different servers or storage systems.
 */
--- Three partition strategies:
        -- Range Partitioning
        -- List Partitioning
        -- Hash Partitioning

--- 1. Range Partitioning
/*
        - Range partitioning is the most common type of partitioning.
        - It divides the data based on a range of values in a column.
        - Each partition contains a contiguous range of values.
        - This strategy is ideal for time-series data or incrementing sequences (maybe a BIGINT primary key), 
          where you partition data based on a range of values (e.g., by day or number of keys).
*/
CREATE TABLE ENGINEER
(
    id bigserial NOT NULL,
    first_name character varying(255) NOT NULL,
    last_name character varying(255) NOT NULL,
    gender smallint NOT NULL,
    country_id bigint NOT NULL,
    title character varying(255) NOT NULL,
    started_date date,
    created timestamp without time zone NOT NULL
) PARTITION BY RANGE(started_date);

CREATE TABLE ENGINEER_Q1_2020 PARTITION OF ENGINEER FOR VALUES FROM ('2020-01-01') TO ('2020-04-01');
    
CREATE TABLE ENGINEER_Q2_2020 PARTITION OF ENGINEER FOR VALUES FROM ('2020-04-01') TO ('2020-07-01');
    
CREATE TABLE ENGINEER_Q3_2020 PARTITION OF ENGINEER FOR VALUES FROM ('2020-07-01') TO ('2020-10-01');
    
CREATE TABLE ENGINEER_Q4_2020 PARTITION OF ENGINEER FOR VALUES FROM ('2020-10-01') TO ('2020-12-31');

--- Usage: 
EXPLAIN SELECT * FROM ENGINEER WHERE started_date = '2020-04-01';
--- Explain output:
Append (cost=0.00..591.69 rows=5 width=37)
  -> Seq Scan on engineer_q2_2020  (cost=0.00..12.75 rows=5 width=37)
        Filter: (started_date = '2020-04-01'::date)

--- Trong trường hợp insert / update 1 reocord out of 2020
UPDATE ENGINEER SET started_date = '2021-01-01';

INSERT INTO ENGINEER(first_name, last_name, gender, country_id, title, started_date, created) 
    VALUES('Hermina', 'Kuhlman', 3, 229, 'Backend Engineer', '2021-09-23', current_timestamp);
--> Query Error
---> Solution: Create a Default partition Table
CREATE TABLE ENGINEER_DEFAULT_PARTITION PARTITION OF ENGINEER DEFAULT;

--- 2. List Partitioning
/*
        - List partitioning divides the data based on a set of values in a column.
        - Each partition contains a list of values.
        - This strategy is useful for partitioning data based on specific values (e.g., by country, department, or product category).
*/

CREATE TABLE ENGINEER
(
    id bigserial NOT NULL,
    first_name character varying(255) NOT NULL,
    last_name character varying(255) NOT NULL,
    gender smallint NOT NULL,
    country_id bigint NOT NULL,
    title character varying(255) NOT NULL,
    started_date date,
    created timestamp without time zone NOT NULL
) PARTITION BY LIST(title);

CREATE TABLE ENGINEER_ENGINEER PARTITION OF ENGINEER FOR VALUES
    IN ('Backend Engineer', 'Frontend Engineer', 'Fullstack Engineer');

CREATE TABLE ENGINEER_BA_QA PARTITION OF ENGINEER FOR VALUES
    IN ('BA', 'QA');
    
CREATE TABLE ENGINEER_DEFAULT PARTITION OF ENGINEER DEFAULT;

--- 3. Hash Partitioning
/*
        - Hash partitioning is a method that distributes data across partitions based on the hash value of a column.
        - Each partition is identified by a hash value, and data with the same hash value is stored in the same partition.
        - This strategy is useful for partitioning data based on non-sequential values (e.g., by country, department, or product category).
*/
CREATE TABLE ENGINEER
(
    id bigserial NOT NULL,
    first_name character varying(255) NOT NULL,
    last_name character varying(255) NOT NULL,
    gender smallint NOT NULL,
    country_id bigint NOT NULL,
    title character varying(255) NOT NULL,
    started_date date,
    created timestamp without time zone NOT NULL
) PARTITION BY HASH(country_id);

CREATE TABLE ENGINEER_P1 PARTITION OF ENGINEER
    FOR VALUES WITH (MODULUS 3, REMAINDER 0);

CREATE TABLE ENGINEER_P2 PARTITION OF ENGINEER
    FOR VALUES WITH (MODULUS 3, REMAINDER 1);
    
CREATE TABLE ENGINEER_P3 PARTITION OF ENGINEER
    FOR VALUES WITH (MODULUS 3, REMAINDER 2);

--- Usage:
EXPLAIN SELECT * FROM ENGINEER WHERE COUNTRY_ID = 1;
--- Explain output:
   Seq Scan on engineer_q2_2020  (cost=0.00..12.75 rows=423 width=37)
     Filter: (country_id = 1)

--- Note 1: Partition pruning
/*
An optimization technique in SQL that improves query performance on partitioned tables. 
It works by ensuring that the query accesses only the relevant partitions instead of scanning the entire table. 
This reduces the amount of data processed and speeds up query execution.

When query a partitioned table, the database examines the query's conditions (e.g., WHERE clause) to determine which partitions contain relevant data.
Only those partitions are scanned, while irrelevant partitions are "pruned" or excluded from the query plan.
*/
--- Turn off partition pruning
SET enable_partition_pruning = off;

EXPLAIN SELECT * FROM ENGINEER WHERE country_id = 1;

--- 4. Multi-level partitioning
CREATE TABLE ENGINEER
(
    id bigserial NOT NULL,
    first_name character varying(255) NOT NULL,
    last_name character varying(255) NOT NULL,
    gender smallint NOT NULL,
    country_id bigint NOT NULL,
    title character varying(255) NOT NULL,
    started_date date,
    created timestamp without time zone NOT NULL
) PARTITION BY RANGE(started_date);

CREATE TABLE ENGINEER_Q1_2020 PARTITION OF ENGINEER FOR VALUES
    FROM ('2020-01-01') TO ('2020-04-01') PARTITION BY LIST(title);
    
CREATE TABLE ENGINEER_DF PARTITION OF ENGINEER DEFAULT;
    
CREATE TABLE ENGINEER_Q1_2020_SE PARTITION OF ENGINEER_Q1_2020 
    FOR VALUES IN ('Backend Engineer', 'Frontend Engineer', 'Fullstack Engineer');
CREATE TABLE ENGINEER_Q1_2020_BA PARTITION OF ENGINEER_Q1_2020 
    FOR VALUES IN ('BA', 'QA');
CREATE TABLE ENGINEER_Q1_2020_DF PARTITION OF ENGINEER_Q1_2020 DEFAULT;

---- Example: 
        EXPLAIN SELECT * FROM ENGINEER WHERE started_date = '2020-02-02' AND title = 'BA';
---- Output:
        Seq Scan on engineer_q1_2020_ba  (cost=0.00..12.75 rows=5 width=37)
          Filter: ((started_date = '2020-02-02'::date) AND ((title)::text = 'BA'::text))

--- Add-ins 1: Sharding
/*
Sharding is a database design technique that divides a large table into smaller, more manageable pieces known as shards. 
Each shard is a sub-table that holds a subset of the data, with each row existing in exactly one shard
Partition key: Primary Key
*/

--- Add-ins 2:
/*
        Each partition is considered a separate table and inherits the characteristics of the parent table. 
        Indexes can be added to individual partitions to enhance query performance, referred to as 'local indexes'. 
        Alternatively, indexes can be added to the parent table, known as 'global indexes'.

        Nếu 1 bảng không có Primary Key, có thể tạo bao nhiêu Partition cũng được
        Nếu 1 bảng có Primary Key, thì chỉ có thể tạo Partition cho các column thuộc PK đó
        Nếu 1 bảng có cột UNIQUE (ex: id begserial UNIQUE NOT NULL), thì chỉ có thể tạo Partition cho cột đó
        
        The essence of partitioning lies in dividing a dataset into independent partitions based on predefined conditions. 
        To adhere to this principle, overlapping keys across partitions are not allowed. 
        In simple terms, a single record cannot belong to more than one partition, and partition keys must neither overlap nor partially intersect with each other.
*/

--------------------------------------------

-- PARTITIONED HYPERTABLE (Timescale)
---- https://www.timescale.com/learn/when-to-consider-postgres-partitioning

/* Set up */
--- Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

--- Create the main table
CREATE TABLE orders (
    customer_id BIGINT NOT NULL,
    order_time TIMESTAMPTZ NOT NULL,
    region TEXT,
    amount NUMERIC,
    PRIMARY KEY (order_time, customer_id) -- Composite key for uniqueness
);

--- Convert the table to a hypertable partitioned by time (daily chunks)
SELECT create_hypertable('orders', by_range('order_time', INTERVAL '1 day'));
/* 
Similar to hashing partition previously presented, you can use space partitioning on the 
[region] to distribute the data across multiple partitions using 'add_dimension'.
*/
SELECT add_dimension('orders', by_hash('region', 4));
/*
   Explanation:
        The [order_time] column is used for time-based partitioning.
        chunk_time_interval => INTERVAL '1 day' specifies that the time-based partitioning should create chunks of data for each day.
        The region column is used for space partitioning, with 4 (four) partitions.
*/

---- Insert data into hypertable
-- Insert some sample data
-- INSERT INTO orders (order_time, customer_id, region, amount)
-- VALUES
--     ('2024-08-01 00:00:00', 1, 'Region A', 600),
--     ('2024-08-01 00:00:00', 2, 'Region A', 580),
--     ('2024-08-01 01:00:00', 1, 'Region B', 610),
--     ('2024-08-01 01:00:00', 2, 'Region C', 590),
--     ('2024-08-02 00:00:00', 1, 'Region B', 620),
--     ('2024-08-02 00:00:00', 2, 'Region C, 570)

---- Query data
SELECT * FROM orders;

---- Check chunks created
-- Show the chunks created by TimescaleDB
SELECT show_chunks('orders');

-- MATERIALIZED VIEW
/*
A materialized view in PostgreSQL is a database object that stores the result of a query physically on disk. 
Unlike a regular view, which dynamically computes the result each time it is queried, a materialized view caches the result, allowing for faster query performance. 
However, the data in a materialized view does not automatically stay up to date and must be refreshed to reflect changes in the underlying tables.
*/
CREATE MATERIALIZED VIEW ENGINEER_MVIEW AS
    SELECT e.first_name, e.last_name, c.country_name
	    FROM ENGINEER e JOIN COUNTRY c ON e.country_id = c.id;

-- Changes to the underlying tables **ARE NOT AUTOMATICALLY REFLECTED**
--> Use REFRESH MATERIALIZED VIEW to update the data.

REFRESH MATERIALIZED VIEW ENGINEER_MVIEW;

-- JOIN
