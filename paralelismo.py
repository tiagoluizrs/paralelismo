#-*- coding:utf-8 -*-
# encoding=utf8

from Queue import Queue
from concurrent import futures
import mysql.connector
import os, csv

file_stop = False
files_queue = Queue(1000)
insert_queue = Queue(1000)

def start_connections(pool_name, host, port, database, user, password, pool_size):
    pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name=pool_name, pool_size=pool_size,
        pool_reset_session=True, host=host,
        port=port, database=database,
        user=user, password=password)

    return pool

def insert_files_queue(files_queue):
    files = os.listdir('arquivos_csvs')

    """Enchendo fila de arquivos com cada
    arquivo pego da pasta arquivos_csvs"""
    for file in files:
        files_queue.put('arquivos_csvs/%s' % file)

def process_file(files_queue, insert_queue):
    while True:
        insert_list = []
        """Pegando um arquivo da fila de arquivos"""
        if files_queue.qsize() == 0:
            time.sleep(20)
            if files_queue.qsize() == 0:
                break

        file = files_queue.get()

        """Pegando arquivo csv com várias linhas
        e colocando-o no modelo adequado para ser
        inserido no banco de dados"""
        with open(file) as f:
			csv_rows = csv.DictReader(f, delimiter=',')
			for csv_row in csv_rows:
				if not linha['nome'] == 'nome':
    				insert_list.append(
    					[csv_row['name'], csv_row['email'], csv_row['age']]
    				)
        """Inserindo na fila insert_queue a lista
        que foi gerada baseada no arquivo csv
        que foi pego na files_queue"""
        insert_queue.put(insert_list)
        os.remove(file)

def insert_data(insert_queue, pool_conn):
    while True:
        if files_queue.qsize() == 0:
            time.sleep(20)
            if files_queue.qsize() == 0:
                break

        insert_item = insert_queue.get()

        try:
            conn = pool_conn.get_connection()
            cursor = conn.cursor()

            cursor.executemany("""INSERT INTO
            dados_pessoais(name, email, age)
            VALUES (%s, %s, %s);""", (insert_item))

            conn.commit()
        except:
            print("Um erro ocorreu com a conexão")
        finally:
            cursor.close()
            conn.close()

def main():
    pool_conn = start_connections(
        pool_name="pool_paralelismo", host="localhost",
        port=3306, database="livro_python",
        user="root", password="123", pool_size=2)

    with futures.ThreadPoolExecutor(max_workers=5) as parallelism:
		try:
			threads = []

			threads.append(parallelism.submit(insert_files_queue, files_queue))

			threads.append(parallelism.submit(process_file, files_queue, insert_queue))
			threads.append(parallelism.submit(process_file, files_queue, insert_queue))

            threads.append(parallelism.submit(insert_data, insert_queue, pool_conn))
            threads.append(parallelism.submit(insert_data, insert_queue, pool_conn))

			futures.wait(threads, return_when='ALL_COMPLETED')

			print("[[main]] &gt;&gt; Processo finalizado")
		except Exception as e:
			print("[[main]] &gt;&gt; Um erro ocorreu. Descrição: %s" % e)

if __name__ == "__main__":
    main()
