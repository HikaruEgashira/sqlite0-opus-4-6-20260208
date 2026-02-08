-- Window running/cumulative aggregates with ORDER BY in OVER clause
CREATE TABLE sales (month TEXT, amount INTEGER);
INSERT INTO sales VALUES ('Jan', 100);
INSERT INTO sales VALUES ('Feb', 200);
INSERT INTO sales VALUES ('Mar', 150);
INSERT INTO sales VALUES ('Apr', 300);

-- Running SUM
SELECT month, amount, SUM(amount) OVER (ORDER BY month) AS running_sum FROM sales;

-- Running COUNT
SELECT month, COUNT(*) OVER (ORDER BY month) AS running_count FROM sales;

-- Running AVG
SELECT month, amount, AVG(amount) OVER (ORDER BY month) AS running_avg FROM sales;

-- Running MIN/MAX
SELECT month, amount, MIN(amount) OVER (ORDER BY month) AS running_min, MAX(amount) OVER (ORDER BY month) AS running_max FROM sales;

-- Running TOTAL
SELECT month, TOTAL(amount) OVER (ORDER BY month) AS running_total FROM sales;

-- With PARTITION BY and ORDER BY
CREATE TABLE dept_sales (dept TEXT, month TEXT, amount INTEGER);
INSERT INTO dept_sales VALUES ('A', 'Jan', 100);
INSERT INTO dept_sales VALUES ('A', 'Feb', 200);
INSERT INTO dept_sales VALUES ('A', 'Mar', 150);
INSERT INTO dept_sales VALUES ('B', 'Jan', 300);
INSERT INTO dept_sales VALUES ('B', 'Feb', 50);

SELECT dept, month, SUM(amount) OVER (PARTITION BY dept ORDER BY month) AS running_sum FROM dept_sales;

-- ORDER BY DESC
SELECT month, amount, SUM(amount) OVER (ORDER BY month DESC) AS running_sum FROM sales;
