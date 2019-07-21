# Paralelismo no Python
Esse é um pequeno script que demonstrará a aplicação do Paralelismo na linguagem de programação Python.

Através desse scripts veremos os conceitos de Workers sendo aplicados na execução de um pequeno processo que executará métodos de forma paralela através da biblioteca *concurrent* que além de permitir o uso do paralelismo também nos permite gerenciar os processos paralelos de forma correta. Também usaremos a biblioteca *Queue* para a aplicação do conceito de filas em nossa aplicação, permitindo que nosso script se comporte de maneira correta na execução do paralelismo. 

Dentro da pasta `arquivos_para_modelo` temos arquivos para serem usados durate a execução do script, recomendo que duplique esses arquivos se quiser que o processo rode por mais tempo, tendo em vista que o conceito aqui é apenas a demonstração do processo paralelo que irá pegar x arquivos ao mesmo tempo e inserir no banco de dados.

Crie um banco de dados em sua máquina e rode a query a seguir para ter uma tabela similar à esta. 

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
