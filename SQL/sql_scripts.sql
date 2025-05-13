 CREATE DATABASE BigDataClass2025;

 USE BigDataClass2025;



CREATE TABLE Sales (
    sale_id INT PRIMARY KEY,
    product_id INT,
    quantity_sold INT,
    sale_date DATE,
    total_price DECIMAL(10, 2)
);


INSERT INTO Sales (sale_id, product_id, quantity_sold, sale_date, total_price) VALUES
(1, 101, 5, '2024-01-01', 2500.00),
(2, 102, 3, '2024-01-02', 900.00),
(3, 103, 2, '2024-01-02', 60.00),
(4, 104, 4, '2024-01-03', 80.00),
(5, 105, 6, '2024-01-03', 90.00);

SELECT * FROM sales;

CREATE TABLE Products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10, 2)
);

INSERT INTO Products (product_id, product_name, category, unit_price) VALUES
(101, 'Laptop', 'Electronics', 500.00),
(102, 'Smartphone', 'Electronics', 300.00),
(103, 'Headphones', 'Electronics', 30.00),
(104, 'Keyboard', 'Electronics', 20.00),
(105, 'Mouse', 'Electronics', 15.00);

SELECT * FROM products;
SELECT product_name, unit_price FROM Products;
SELECT sale_id, sale_date FROM Sales;

SELECT * FROM Sales;
SELECT * FROM Sales WHERE total_price > 100;

SELECT * FROM products WHERE category = 'Electronics';

SELECT sale_id, total_price FROM Sales WHERE sale_date = '2024-01-03';

SELECT product_id, product_name FROM products WHERE unit_price > 100;

SELECT SUM(total_price) AS total_revenue FROM Sales;

SELECT AVG(unit_price) AS average_unit_price FROM Products;

SELECT SUM(quantity_sold) AS total_quantity_sold FROM Sales;

SELECT sale_date, COUNT(*) AS sales_count FROM Sales GROUP BY sale_date ORDER BY sale_date;

SELECT sale_id, product_id, total_price FROM Sales WHERE quantity_sold > 4;

SELECT ROUND(SUM(total_price), 2) AS total_sales FROM Sales;

SELECT AVG(total_price) AS average_total_price FROM Sales;

SELECT sale_id, DATE_FORMAT(sale_date, '%Y-%b-%d') AS formatted_date FROM Sales;

SELECT SUM(Sales.total_price) AS total_revenue FROM Sales 
JOIN Products ON Sales.product_id = Products.product_id WHERE Products.category = 'Electronics';

SELECT product_name, unit_price FROM Products WHERE unit_price BETWEEN 20 AND 600;

SELECT product_name, category FROM Products ORDER BY category ASC;

SELECT SUM(quantity_sold) AS total_quantity_sold FROM Sales 
JOIN Products ON Sales.product_id = Products.product_id WHERE Products.category = 'Electronics';

SELECT product_name, quantity_sold * unit_price AS total_price FROM Sales 
JOIN Products ON Sales.product_id = Products.product_id;

SELECT product_id, COUNT(*) AS sales_count FROM Sales 
GROUP BY product_id ORDER BY sales_count DESC LIMIT 1;

SELECT product_id, product_name FROM Products 
WHERE product_id NOT IN (SELECT DISTINCT product_id FROM Sales);

SELECT p.category, SUM(s.total_price) AS total_revenue FROM Sales s
JOIN Products p ON s.product_id = p.product_id GROUP BY p.category;

SELECT category FROM Products GROUP BY category
ORDER BY AVG(unit_price) DESC LIMIT 1;

