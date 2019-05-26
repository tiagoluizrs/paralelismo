#-*- coding:utf-8 -*-
# encoding=utf8

from concurrent import futures
from mysql.connector import pooling
import os, csv, mysql.connector, ipdb
from queue import Queue

"""Caminho padrão do projeto"""
PROJECT_FOLDER = os.path.dirname(os.path.abspath(__file__))

"""Tamanho máximo que cada fila comporta"""
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
	global PROJECT_FOLDER

	try:
		files = os.listdir('%s/arquivos_csvs' % PROJECT_FOLDER)
	except Exception as e:
		print(e)

	"""Enchendo fila de arquivos com cada
	arquivo pego da pasta arquivos_csvs"""
	for file in files:
		try:
			files_queue.put('%s/arquivos_csvs/%s' % (PROJECT_FOLDER, file))
		except Exception as e:
			print(e)

def process_file(files_queue, insert_queue):
	global PROJECT_FOLDER

	while True:
		insert_list = []
		"""Pegando um arquivo da fila de arquivos"""
		if files_queue.qsize() == 0:
			if len(os.listdir('%s/arquivos_csvs' % PROJECT_FOLDER)) == 0:
				break
			else:
				pass
		else:
			file = files_queue.get()

			"""Pegando arquivo csv com várias linhas
			e colocando-o no modelo adequado para ser
			inserido no banco de dados"""
			try:
				with open(file) as f:
					csv_rows = csv.DictReader(f, delimiter=';')
					for csv_row in csv_rows:
						if not csv_row['name'] == 'name':
							insert_list.append([csv_row['name'], csv_row['email'], csv_row['age']])
			except Exception as e:
				print(e)

			"""Inserindo na fila insert_queue a lista
			que foi gerada baseada no arquivo csv
			que foi pego na files_queue"""
			insert_queue.put(insert_list)
			os.remove(file)

def insert_data(insert_queue, pool_conn):
	global PROJECT_FOLDER

	while True:
		if insert_queue.qsize() == 0:
			if len(os.listdir('%s/arquivos_csvs' % PROJECT_FOLDER)) == 0:
				break
			else:
				pass
		else:
			insert_item = insert_queue.get()

			try:
				conn = pool_conn.get_connection()
				cursor = conn.cursor()

				cursor.executemany("""INSERT INTO
				dados_pessoais(name, email, age)
				VALUES (%s, %s, %s);""", (insert_item))

				conn.commit()
			except Exception as e:
				print("Um erro ocorreu com a conexão %s" % e)
			finally:
				cursor.close()
				conn.close()

def main():
	pool_conn = start_connections(
		pool_name="pool_paralelismo", host="localhost",
		port=3306, database="livro_python",
		user="root", password="123", pool_size=3)

	try:
		with futures.ThreadPoolExecutor(max_workers=7) as parallelism:
			try:
				threads = []

				threads.append(parallelism.submit(insert_files_queue, files_queue))

				threads.append(parallelism.submit(process_file, files_queue, insert_queue))
				threads.append(parallelism.submit(process_file, files_queue, insert_queue))
				threads.append(parallelism.submit(process_file, files_queue, insert_queue))

				threads.append(parallelism.submit(insert_data, insert_queue, pool_conn))
				threads.append(parallelism.submit(insert_data, insert_queue, pool_conn))
				threads.append(parallelism.submit(insert_data, insert_queue, pool_conn))

				futures.wait(threads, return_when='ALL_COMPLETED')
			except Exception as e:
				print("Um erro ocorreu. %s" % e)
	except Exception as e:
		print("Um erro ocorreu. %s" % e)

if __name__ == "__main__":
	main()
