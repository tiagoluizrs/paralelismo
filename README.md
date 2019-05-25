# paralelismo
Pequeno script que demonstra como funciona o processo de paralelismo no Python.

Crie um banco de dados similar ao do exemplo à seguir:
```mysql
mysql>  CREATE TABLE dados_pessoais(
    ->   id INT(11) AUTO_INCREMENT,
    ->   name VARCHAR(255) NOT NULL,
    ->   email VARCHAR(100),
    ->   age TINYINT(4),
    ->   date_created DATETIME DEFAULT CURRENT_TIMESTAMP,
    ->   PRIMARY KEY (id)
    -> ); 
````
