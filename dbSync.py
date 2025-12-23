#!/usr/bin/python3
#coding=utf-8

########################################################################
# Скрипт синхронизации таблиц БД
# Версия 1.2
# Скрипач: rootik
########################################################################


import argparse
import yaml
from yaml.loader import SafeLoader
import sqlalchemy as sa
import oracledb
import csv
import os
from datetime import datetime, timedelta
from decimal import Decimal
from collections import namedtuple
import logging

#Раскрываем настройки для коннекта
def replace_connects(sync_conf, conn_config):

	portable_attrs = ['scheme_name', 'postfix', 'db_user', 'db_password', 'db_host', 'db_port', 'db_name']
	
	for db in ['local_db', 'remote_db']:
		db_conf = conn_config[sync_conf[db]]
		#Если используется dblink, добавляем креды для бд, из которой он доступен
		if ('dblink' in db_conf) and (db_conf['dblink'] == True) and ('avail_from' in db_conf):
			db_conf = {**db_conf, **conn_config[db_conf['avail_from']]}
			
		sync_conf[db] = {}	
		for attr in portable_attrs:
			if attr in db_conf:
				sync_conf[db][attr] = db_conf[attr]
			else:
				sync_conf[db][attr] = ''
		
		if sync_conf[db]['db_port'] == '':
			sync_conf[db]['db_port'] = 1521
		if sync_conf[db]['scheme_name'] != '':
			sync_conf[db]['scheme_name'] = sync_conf[db]['scheme_name']+'.'
		if sync_conf[db]['postfix'] != '':
			sync_conf[db]['postfix'] = '@'+sync_conf[db]['postfix']
	return(sync_conf)
	
#Установка соединения с БД
def get_db_connection(conn_conf, get_dsn=False):
	
	required_attrs = ['db_user', 'db_password', 'db_host', 'db_name', 'db_port']
	
	try:
		for attr in required_attrs:
			if (attr not in conn_conf) or (conn_conf[attr] == ''):
				raise BaseException('The required attribute '+attr+' could not be found')
		dsn = f"oracle+oracledb://{conn_conf['db_user']}:{conn_conf['db_password']}@{conn_conf['db_host']}/?service_name={conn_conf['db_name']}"
		if get_dsn:
			return dsn
		engine = sa.create_engine(dsn)
		return engine
	except BaseException as e:
		logging.error(str(e))
		return None
		
#Запрос select к БД для большой таблицы
def get_big_table_data(engine, columns, table, where=''):
	try:
		logging.debug(f"SELECT {','.join(columns)} FROM {table} {where}")
		with engine.connect() as conn:
			result = conn.execution_options(stream_results=True).execute(sa.text(f"SELECT {','.join(columns)} FROM {table} {where}"))
			col_names = [col.upper() for col in result.keys()]
			Row = namedtuple('Row', col_names)
			
			while True:
				rows = result.fetchmany(5000)
				if not rows:
					break
					
				for row in rows:
					yield Row(*row)
					
	except BaseException as e:
		logging.error('Error executing request:')
		logging.error(f"SELECT {','.join(columns)} FROM {table} {where}")
		logging.error(str(e))
		return None
		
#Запрос select к БД
def get_table_data(engine, columns, table, where=''):
	try:
		logging.debug(f"SELECT {','.join(columns)} FROM {table} {where}")
		with engine.connect() as conn:
			result = conn.execute(sa.text(f"SELECT {','.join(columns)} FROM {table} {where}"))
			data = [
					dict(row._mapping)
					for row in result
			]
	except BaseException as e:
		logging.error('Error executing request:')
		logging.error(f"SELECT {','.join(columns)} FROM {table} {where}")
		logging.error(str(e))
		data = None
		
	#Названия столбцов выгружаются в нижнем регистре, поднимаем в верхний
	if data != None:
		tmp_list = []
		for row in data:
			tmp_list.append({key.upper(): row[key] for key in row})
		data = tmp_list
	
	return data

#Выполнение произвольного запроса	
def exec_query(engine, query):
	try:
		logging.debug(query)
		with engine.connect() as conn:
			result = conn.execute(sa.text(query))
			conn.commit()
		return True
	except BaseException as e:
		logging.error('Error executing request:')
		logging.error(query)
		logging.error(str(e))
		result = None	
	return result

#Вставка данных в таблицу	
def insert_table_data(engine, columns, table, insert_data):
	try:
		query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(f':{column}' for column in columns)})"
		logging.debug(query)
		batch = []
		with engine.begin() as conn:
			for row in insert_data:
				batch.append({column: getattr(row,column) for column in columns})
				if len(batch) >= 5000:
					conn.execute(sa.text(query), batch)
					batch.clear()
					
			if batch:
				conn.execute(sa.text(query), batch)
		return True
	except BaseException as e:
		logging.error('Error executing request:')
		logging.error(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(f':{column}' for column in columns)})")
		logging.error(str(e))
		return None
		
def format_data_type(col):
	data_type = col["DATA_TYPE"]
	
	if data_type in ("VARCHAR2", "CHAR", "NVARCHAR2"):
		return f"{data_type}({col['DATA_LENGTH']})"
		
	if data_type == "NUMBER":
		if col["DATA_PRECISION"] != None:
			if col["DATA_SCALE"] != None:
				return f"NUMBER({col['DATA_PRECISION']},{col['DATA_SCALE']})"
			return f"NUMBER({col['DATA_PRECISION']})"
		return "NUMBER"
		
	return data_type
	
def build_columns_ddl(columns):
	ddl = []
	
	for col in sorted(columns, key=lambda c: c["COLUMN_ID"]):
		line = f'    {col["COLUMN_NAME"]} {format_data_type(col)}'
		
		if col["NULLABLE"] == "N":
			line += " NOT NULL"
			
		ddl.append(line)
		
	return ddl
	
def build_constrains_ddl(constrains, cons_columns):
	cols_by_cons = {}
	for c in cons_columns:
		cols_by_cons.setdefault(c["CONSTRAINT_NAME"], []).append((c["POSITION"], c["COLUMN_NAME"]))
		
	ddl = []
	
	for cons in constrains:
		ctype = cons["CONSTRAINT_TYPE"]
		cname = cons["CONSTRAINT_NAME"]
		
		if ctype not in ("P", "U", "R", "C"):
			continue
			
		cols = [col for _, col in sorted(cols_by_cons.get(cname, []))]
		if ctype == "P":
			ddl.append(f"    CONSTRAINT {cname} PRIMARY KEY ({', '.join(cols)})")
		elif ctype == "U":
			ddl.append(f"    CONSTRAINT {cname} UNIQUE ({', '.join(cols)})")
		elif ctype == "R":
			ddl.append(
						f"    CONSTRAINT {cname} FOREIGN KEY ({', '.join(cols)}) "
						f"REFERENCES {cons['R_OWNER']}.{cons['R_CONSTRAINT_NAME']}"
						)
		elif ctype == "C" and cons["SEARCH_CONDITION"]:
			ddl.append(f"    CONSTRAINT {cname} CHECK ({cons['SEARCH_CONDITION']})")
	return ddl

#Собираем ddl	
def get_ddl(columns_conf, remote_table, local_table):
	columns = get_table_data(remote_table['engine'], ['*'], 'all_tab_columns'+remote_table['postfix'], f" WHERE table_name='{remote_table['name']}'")

	constraints = get_table_data(remote_table['engine'], ['*'], 'all_constraints'+remote_table['postfix'], f" WHERE table_name='{remote_table['name']}'")

	cons_columns = get_table_data(remote_table['engine'], ['*'], 'all_cons_columns'+remote_table['postfix'], f" WHERE table_name='{remote_table['name']}'")
	
	col_ddls = build_columns_ddl(columns)
	
	cons_ddls = build_constrains_ddl(constraints, cons_columns)
    
	#all_defs = col_ddls + cons_ddls
	all_defs = col_ddls
    
	ddl = (
        f"CREATE TABLE {local_table['name']} (\n"
        + ",\n".join(all_defs)
        + "\n)"
	)
    
	return ddl
 
		
#Создаем таблицу	
def create_table(columns_conf, local_conf, remote_conf, one_db, show_only):
	
	if one_db:
		if columns_conf['map_columns'] == {}:
			columns_str = '*'
		else:	
			columns_str = ','.join(map_columns(columns_conf))
		query = f"CREATE TABLE {local_conf['prefix']}{local_conf['name']}{local_conf['postfix']} AS (SELECT {columns_str} FROM {remote_conf['prefix']}{remote_conf['name']}{remote_conf['postfix']})"
	else:
		query = get_ddl(columns_conf, remote_conf, local_conf)
	if show_only == 'yes':
		answer = query
	else:
		answer = exec_query(local_conf['engine'],query)
		if answer != None:
			if columns_conf['map_columns'] != {}:
				for column in columns_conf['map_columns']:
					query = f"ALTER TABLE {local_conf['prefix']}{local_conf['name']}{local_conf['postfix']} RENAME COLUMN {columns_conf['map_columns'][column]} TO {column}"
					answer = exec_query(local_conf['engine'],query)
					if answer == None:
						break
	return answer

#Синхронизация через truncate
def truncate_sync(columns_conf, local_conf, remote_conf, one_db):
	answer = exec_query(local_conf['engine'],
						f"TRUNCATE TABLE {local_conf['prefix']}{local_conf['name']}{local_conf['postfix']}")
	if answer != None:
		if one_db:
			query = f"INSERT INTO {local_conf['prefix']}{local_conf['name']}{local_conf['postfix']} ({','.join(columns_conf['local_columns'])}) SELECT {','.join(columns_conf['remote_columns'])} FROM {remote_conf['prefix']}{remote_conf['name']}{remote_conf['postfix']}"
			
			answer = exec_query(local_conf['engine'],query)
		else:
			answer = insert_table_data(local_conf['engine'], columns_conf['local_columns'], f"{local_conf['prefix']}{local_conf['name']}{local_conf['postfix']}", remote_conf['data'])
		#Пробуем вернуть все в зад, если при в ставке данных из удаленной таблицы возникла ошибка
		if answer == None:
			exec_query(local_conf['engine'],
						f"TRUNCATE TABLE {local_conf['prefix']}{local_conf['name']}{local_conf['postfix']}")
			insert_table_data(local_conf['engine'], columns_conf['local_columns'], f"{local_conf['prefix']}{local_conf['name']}{local_conf['postfix']}", local_conf['data'])
		
	return answer
		
	
def answer_to_strlist(answer):
	result_list = []
	for element in answer:
		result_list.append(element[list(element.keys())[0]])	
	return result_list

#Получаем столбцы таблиц
def get_tables_columns(conf, local_conf, remote_conf):
	
	result = {'remote_columns' : [],
			'local_columns' : [],
			'map_columns' : {},
			'identity_local_columns' : [],
			'lines_count' : 0}
	#Столбцы удаленной таблицы		
	answer = get_table_data(remote_conf['engine'],
									['column_name'],
									'all_tab_columns'+remote_conf['postfix'],
									f"WHERE table_name='{remote_conf['name']}'")
	if (answer == None) or (answer == []):
		result['remote_columns'] = answer
		return result
	result['remote_columns'] = answer_to_strlist(answer)
	#Проверяем существование локальной таблицы
	answer = get_table_data(local_conf['engine'],
							['table_name'],
							'all_tables'+local_conf['postfix'],
							f"WHERE table_name='{local_conf['name']}'")
	if (answer == None) or (answer == []):
		result['local_columns'] = answer
	else:
		#Получаем кол-во строк таблицы
		answer = get_table_data(local_conf['engine'],
								['COUNT(*)'],
								f"{local_conf['prefix']}{local_conf['name']}{local_conf['postfix']}")
		result['lines_count'] = answer[0]['COUNT(*)']
		#Получаем столбцы, отдельно идентити
		answer = get_table_data(local_conf['engine'],
								['column_name'],
								'all_tab_columns'+local_conf['postfix'],
								f"WHERE table_name='{local_conf['name']}' AND IDENTITY_COLUMN='NO'")
		result['local_columns'] = answer_to_strlist(answer)
		
		answer = get_table_data(local_conf['engine'],
								['column_name'],
								'all_tab_columns'+local_conf['postfix'],
								f"WHERE table_name='{local_conf['name']}' AND IDENTITY_COLUMN='YES'")
		result['identity_local_columns'] = answer_to_strlist(answer)
		#Выкидываем из столбцов удаленной таблицы те, которые в локальной идентити
		for column in result['identity_local_columns']:
			if column in result['remote_columns']:
				result['remote_columns'].remove(column)
	
	if 'map_columns' in conf:
		result['map_columns'] = conf['map_columns']
		#Выкидываем из смапленных столбцов те, которые идентити
		for column in result['identity_local_columns']:
			if column in conf['map_columns']:
				del result['map_columns'][column]
		#Если синхроним только то, что смаплено, выкидываем все остальное
		if ('only_mapped' in conf) and (conf['only_mapped'] == True):
			tmp_list = []
			for column in result['remote_columns']:
				if column in result['map_columns'].values():
					tmp_list.append(column)
			result['remote_columns'] = tmp_list
			if result['local_columns'] != []:
				tmp_list = []
				for column in result['local_columns']:
					if column in result['map_columns'].keys():
						tmp_list.append(column)
				result['local_columns'] = tmp_list			
	return result

#Маппим столбцы для запроса	
def map_columns(columns_conf):
	columns = []
	for column in columns_conf['remote_columns']:
		if column in columns_conf['map_columns'].values():
			tmp_str = next(key for key, value in columns_conf['map_columns'].items() if value == column)
			if column != tmp_str:
				column = column+' AS '+tmp_str
		columns.append(column)
	return columns


#Сравнение таблиц
def compare_tables(remote, local, columns, one_db):
	if 'diff_keys' in columns:
		key_columns = tuple(columns['diff_keys'])
		if columns['map_columns'] != {}:
			remote_columns = []
			for column in columns['diff_keys']:
				if column in columns['map_columns']:
					column = f"{columns['map_columns'][column]} AS {column}"
				remote_columns.append(column)
	else:
		key_columns = tuple(columns['local_columns'])
		remote_columns = tuple(columns['local_columns'])
	if one_db:
		minus_query = f"{local['prefix']}{local['name']}{local['postfix']} MINUS SELECT {','.join(remote_columns)} FROM {remote['prefix']}{remote['name']}{remote['postfix']}"
		minus_result = get_big_table_data(local['engine'], key_columns, minus_query)
		for row in minus_result:
			yield row
	else:
		local_keys = set()

		for row in local['data']:
			local_keys.add(tuple(getattr(row, key) for key in key_columns))
	
		for row in remote['data']:
			key = tuple(getattr(row, key) for key in key_columns)
		
			if key not in local_keys:
				yield row

#Удаление старых файлов	
def delete_old_backups(tablename, deadline, config):
	
	if 'backup_path' in config:
		filename = config['backup_path']+'/'+tablename+'/'
	else:
		filename = './backup'+'/'+tablename+'/'
	
	files = os.listdir(filename)
	
	dead_date = datetime.now() - timedelta(days=deadline)
	
	for f in files:
		file_date = datetime.strptime(f, "%Y-%m-%d_%H%M%S")
		if file_date < dead_date:
			os.remove(filename+f)

#Создание нехватающих локальних директорий
def check_local_path(path):
	exists_path = '.'
	for path_part in path.split('/')[1:-1]:
		exists_path += '/'+path_part
		if not (os.path.exists(exists_path)):
			os.mkdir(exists_path)
			
#Создание csv файла с данными
def make_csv(data, tablename, config):
	if 'backup_path' in config:
		filename = config['backup_path']+'/'+tablename+'/'+datetime.now().strftime("%Y-%m-%d_%H%M%S")
	else:
		filename = './backup'+'/'+tablename+'/'+datetime.now().strftime("%Y-%m-%d_%H%M%S")
	check_local_path(filename)
	try:
		with open(filename, "w", newline="", encoding="utf-8") as f:
			writer = csv.writer(
				f,
				delimiter=";"
			)
			first_row = True
			for row in data:
				if first_row:
					writer.writerow(row._fields)
				first_row = False
				
				writer.writerow(row)
				
	except BaseException as e:
		logging.error('Возникла ошибка при создании csv файла '+filename)
		logging.error(str(e))
		filename = None
	
	return filename
	
def sync_tables(yml_config, show_only):
	
	general_config = yml_config['General']
	conn_config = yml_config['Connections']
	
	config = yml_config['Sync']
	
	for sync in config:
		config[sync] = replace_connects(config[sync], conn_config)
		#Пробуем подключиться к базам
		local_engine = get_db_connection(config[sync]['local_db'])
		if (get_db_connection(config[sync]['remote_db'], True) == get_db_connection(config[sync]['local_db'], True)):
			remote_engine = local_engine
			one_db_query = True
		else:	
			remote_engine = get_db_connection(config[sync]['remote_db'])
			one_db_query = False
		
		if (remote_engine == None) or (local_engine == None):
			logging.critical(' Failed to establish a connection to one of the databases for syncing '+sync)
			continue

		for table in config[sync]['tables']:
			#Получаем имена таблиц с префиксами и без
			local_table = { 'name' : list(table.keys())[0],
								'prefix' : config[sync]['local_db']['scheme_name'],
								'postfix' : config[sync]['local_db']['postfix'],
								'engine' : local_engine}
			remote_table = { 'name' : table[local_table['name']],
								'prefix' : config[sync]['remote_db']['scheme_name'],
								'postfix' : config[sync]['remote_db']['postfix'],
								'engine' : remote_engine}
			#Получаем информацию о столбцах таблиц
			tables_columns = get_tables_columns(table, local_table, remote_table)
			
			if (tables_columns['remote_columns'] == None) or (tables_columns['remote_columns'] == []) or (tables_columns['local_columns'] == None):
				logging.error(' An error occurred while synchronizing the table '+local_table['name'])
				continue
			
			#Получаем данные из таблиц
			if (tables_columns['local_columns'] != []):
				local_table['data'] = get_big_table_data(local_table['engine'],
											tables_columns['local_columns'],
											local_table['prefix']+local_table['name']+local_table['postfix'])
			
			if not one_db_query:
				remote_table['data'] = get_big_table_data(remote_table['engine'],
											map_columns(tables_columns),
											remote_table['prefix']+remote_table['name']+remote_table['postfix'])
				
			#Создаем таблицу, если она не существует	
			if tables_columns['local_columns'] == []:
				create_result = create_table(tables_columns, local_table, remote_table, one_db_query, show_only)
				if show_only == 'yes':
					logging.info('The specified table is not in the database. To create it, the following query will be used:')
					logging.info(create_result)
					continue
				if create_result == None:
					logging.error('An error occurred while trying to create the table '+local_table['name'])
					continue
				else:
					tables_columns = get_tables_columns(table, local_table, remote_table)
					local_table['data'] = get_big_table_data(local_table['engine'],
											tables_columns['local_columns'],
											local_table['prefix']+local_table['name']+local_table['postfix'])

			#Скидываем бэкап
			if (('backup' in config[sync]) and (config[sync]['backup'] == True) and (show_only != 'yes')):
				backup_result = make_csv(local_table['data'], local_table['name'], general_config)
				local_table['data'] = get_big_table_data(local_table['engine'],
														tables_columns['local_columns'],
														local_table['prefix']+local_table['name']+local_table['postfix'])
				if ('rotate' in config[sync]):
					delete_old_backups(local_table['name'], config[sync]['rotate'], general_config)

			#Синхронизация через сравнение	
			if (config[sync]['sync_type'] == 'diff') or (show_only == 'yes'):
				#Сравнение полученных данных
				if 'diff_key' in table:
					tables_columns['diff_key'] = table['diff_key']
		
				comparison = compare_tables(remote_table,
											local_table,
											tables_columns,
											one_db_query)
											
				if (show_only == 'yes'):
					print('')
					print(f'================== diff results fot {local_table["name"]} =========================')
					print('')
					print(f'Total lines: {tables_columns["lines_count"]}')
					tmp_list = []
					for row in comparison:
						tmp_list.append(row)
					print(f'Only in remote lines: {len(tmp_list)}')
					print('')
					continue
				else:
					insert_result = insert_table_data(local_table['engine'], tables_columns['local_columns'], local_table['name'], comparison)
					if insert_result == None:
						logging.error(' An error occurred while trying to insert data into the table '+local_table['name'])
			#Синхронизация через полную очистку		
			if config[sync]['sync_type'] == 'truncate':
				truncate_result = truncate_sync(tables_columns, local_table, remote_table, one_db_query)

def get_log_level(level_str):
	try:
		return getattr(logging, level_str.upper())
	except AttributeError:
		raise ValueError(f"Invalid log level: {level_str}")

def setup_logging(config, cmd_arg):
	log_level_str = config['General'].get('log_level', 'INFO')
	if cmd_arg != '-':
		log_level_str = cmd_arg
	log_level = get_log_level(log_level_str)
	
	log_file = config['General'].get('log_file')
	
	handlers = []
	
	# Лог в файл (если задан)
	if log_file:
		handlers.append(logging.FileHandler(log_file))

	# Всегда лог в stdout
	handlers.append(logging.StreamHandler())

	logging.basicConfig(
		level=log_level,
		format='%(asctime)s %(levelname)s %(name)s: %(message)s',
		handlers=handlers
	)

	# Подавляем шум SQLAlchemy при INFO
	if log_level > logging.DEBUG:
		logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

if __name__ == '__main__':
	
	start_time = datetime.now()
	
	#Разбираем параметры командной строки
	cmd_parser = argparse.ArgumentParser()
	cmd_parser.add_argument('-c' ,'--config', default='config.yml')

	cmd_parser.add_argument('-i','--input', dest='remote_table', help='remote table')
	cmd_parser.add_argument('-o','--output', dest='local_table', help='local table')
	cmd_parser.add_argument('-l','--local-conn', dest='local_db_name', default='pl_db', help='local db')
	cmd_parser.add_argument('-r','--remote-conn', dest='remote_db_name', default='prod', help='remote db')
	cmd_parser.add_argument('-m','--method-sync', dest='sync_type', default='truncate', choices=['truncate', 'diff'], help='sync type')
	cmd_parser.add_argument('-s','--show-only', dest='show_only', default='no', choices=['yes', 'no'], help='only show tables diffs')
	cmd_parser.add_argument('-ll','--log-level', dest='log_level', default='-', choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], help='log level')
	
	cmd_args = cmd_parser.parse_args()
	
	#Разбираем конфиг файл
	with open(cmd_args.config) as yml:
		yml_config = yaml.load(yml, Loader=SafeLoader)
	
	setup_logging(yml_config, cmd_args.log_level)
	
	logging.info(f'================== the script started working {start_time.strftime("%d/%m/%Y %H:%M")} =========================')
	
	if (cmd_args.remote_table != None) and (cmd_args.local_table != None):
		yml_config['Sync'] = {'manual_sync': 
									{'local_db': cmd_args.local_db_name, 
									'remote_db': cmd_args.remote_db_name, 
									'sync_type': cmd_args.sync_type, 
									'backup': True, 
									'rotate': 1, 
									'tables': [{ cmd_args.local_table : cmd_args.remote_table }]}}
	sync_tables(yml_config, cmd_args.show_only)
	
	logging.info(f'================== The script execution time was: {(datetime.now() - start_time)} =========================')
