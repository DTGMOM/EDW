
CREATE DATABASE university_db;
USE university_db;

CREATE TABLE users (
 id INT AUTO_INCREMENT PRIMARY KEY,
 username VARCHAR(100),
 password VARCHAR(100),
 role VARCHAR(50)
);

CREATE TABLE papers (
 id INT AUTO_INCREMENT PRIMARY KEY,
 title VARCHAR(255),
 description TEXT,
 status VARCHAR(50),
 user_id INT
);
