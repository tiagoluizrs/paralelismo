#-*- coding:utf-8 -*-

from concurrent import futures
from mysql.connector import pooling
import os, csv, ipdb, logging
from queue import Queue

"""Caminho padrão do projeto"""
PROJECT_FOLDER = os.path.dirname(os.path.abspath(__file__))

"""Tamanho máximo que cada fila comporta"""
files_queue = Queue(1000)
insert_queue = Queue(1000)


def start_connections(pool_name, host, port, database, user, password, pool_size):
	pool = pooling.MySQLConnectionPool(
		pool_name=pool_name, pool_size=pool_size, host=host,
		port=port, database=database,
		user=user, password=password)

	return pool

def insert_files_queue(files_queue, process):
	global PROJECT_FOLDER

	try:
		files = os.listdir('%s/arquivos_csvs' % PROJECT_FOLDER)
	except Exception as e:
		logging.error("[Main | insert_files_queue - listdir ] >> Um erro inesperado ocorreu. Descrição: %s" % e)

	"""Enchendo fila de arquivos_csvs com cada
	arquivo pego da pasta arquivos_csvs"""
	for file in files:
		try:
			files_queue.put('%s/arquivos_csvs/%s' % (PROJECT_FOLDER, file))
			logging.info("Pegando arquivo na pasta e inserindo na fila 'files_queue'. Processo número: %s" % process)
		except Exception as e:
			logging.error("[Main | insert_files_queue - files ] >> Um erro inesperado ocorreu. Descrição: %s" % e)

def process_file(files_queue, insert_queue, process):
	global PROJECT_FOLDER

	while True:
		insert_list = []
		"""Pegando um arquivo da fila de arquivos_csvs"""
		if files_queue.qsize() == 0:
			if len(os.listdir('%s/arquivos_csvs' % PROJECT_FOLDER)) == 0:
				break
			else:
				pass
		else:
			file = files_queue.get()
			logging.info("Pegando arquivo na fila de arquivos_csvs 'files_queue'. Processo número: %s" % process)

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
				logging.error("[Main | process_file - open csv ] >> Um erro inesperado ocorreu. Descrição: %s" % e)

			"""Inserindo na fila insert_queue a lista
			que foi gerada baseada no arquivo csv
			que foi pego na files_queue"""
			insert_queue.put(insert_list)
			logging.info("Inserindo arquivo na fila 'insert_queue'. Processo número: %s" % process)
			os.remove(file)

def insert_data(insert_queue, pool_conn, process):
	global PROJECT_FOLDER

	while True:
		if insert_queue.qsize() == 0:
			if len(os.listdir('%s/arquivos_csvs' % PROJECT_FOLDER)) == 0:
				break
			else:
				pass
		else:
			insert_item = insert_queue.get()
			logging.info("Pegando arquivo na fila de arquivos_csvs 'insert_queue'. Processo número: %s" % process)

			try:
				conn = pool_conn.get_connection()
				cursor = conn.cursor()

				cursor.executemany("""INSERT INTO
				dados_pessoais(name, email, age)
				VALUES (%s, %s, %s);""", (insert_item))

				conn.commit()
				logging.info("Dados do arqivo inseridos no banco de dados. Processo número: %s" % process)
			except Exception as e:
				logging.error("[Main | insert_data ] >> Um erro ocorreu com a conexão %s. Processo número: %s" % (e, process))
			finally:
				cursor.close()
				conn.close()

def main():
	logging.info("Iniciando processo.")

	pool_conn = start_connections(
		pool_name="pool_paralelismo", host="localhost",
		port=3306, database="livro_python",
		user="root", password="12345678", pool_size=3)

	try:
		with futures.ThreadPoolExecutor(max_workers=7) as parallelism:
			try:
				threads = []

				threads.append(parallelism.submit(insert_files_queue, files_queue, 1))

				threads.append(parallelism.submit(process_file, files_queue, insert_queue, 1))
				threads.append(parallelism.submit(process_file, files_queue, insert_queue, 2))
				threads.append(parallelism.submit(process_file, files_queue, insert_queue, 3))

				threads.append(parallelism.submit(insert_data, insert_queue, pool_conn, 1))
				threads.append(parallelism.submit(insert_data, insert_queue, pool_conn, 2))
				threads.append(parallelism.submit(insert_data, insert_queue, pool_conn, 3))

				futures.wait(threads, return_when='ALL_COMPLETED')
			except Exception as e:
				logging.error("[Main | main - creating threads ] >> Um erro ocorreu com a conexão %s." % e)
	except Exception as e:
		logging.error("[Main | start threads ] >> Um erro ocorreu com a conexão %s." % e)

if __name__ == "__main__":
	try:
		logging.basicConfig(filename='log.log', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
	except:
		print("Um erro ocorreu ao criar o log.")

	main()
