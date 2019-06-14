import pandas as pd
from sqlalchemy import create_engine


def sql_read(query: str, database: str = "fantasy_football"):
	"""
	Performs a MySQL query on a given database
	:param query: Query string
	:param database: Database on which to perform the query
	:return: Query result as a pandas dataframe
	"""
	con = create_engine("mysql+mysqldb://root:@localhost/{database}".format(database=database))
	df = pd.read_sql(query, con=con)
	con.dispose()
	return df


def sql_write(dataframe: pd.DataFrame, table_name: str, database: str = "fantasy_football", if_exists: str = "replace"):
	"""
	Writes a pandas dataframe to a table on a MySQL database
	:param dataframe: Pandas dataframe
	:param table_name: Table name to write to
	:param database: Database to write to
	:param if_exists: Action to perform if the table already exists
	:return: None
	"""
	con = create_engine("mysql+mysqldb://root:@localhost/{database}".format(database=database))
	dataframe.to_sql(con=con, name=table_name, if_exists=if_exists, index=False)
	con.dispose()


def attach_timestamp(dataframe: pd.DataFrame):
	"""
	Attaches the current timestamp to a pandas dataframe
	:param dataframe: A pandas dataframe
	"""
	dataframe.insert(0, 'timestamp', pd.datetime.now().replace(microsecond=0))


def sql_append_new_rows(dataframe: pd.DataFrame, table_name: str, key: list, database: str = "fantasy_football"):
	"""
	Compares a pandas dataframe to an existing MySQL table, for a given set of columns. Only the rows that don't exist
	in the MySQL table, or are modified, are appended to the MySQL table.
	:param dataframe: A pandas dataframe
	:param table_name: Table name to write to
	:param key: The columns to compare for changes/additions
	:param database: The database to write to
	:return: None
	"""
	if not set(key).issubset(dataframe.columns.to_list()):
		raise Exception("Keys not in dataframe")
	existing_data = sql_read("SELECT * FROM {database}.{table_name}".format(database=database, table_name=table_name))
	if not set(key).issubset(existing_data.columns.to_list()):
		raise Exception("Keys not in SQL table")
	new_data = dataframe[~dataframe[key].isin(existing_data[key])]
	sql_write(new_data, table_name, database, if_exists="append")


def write_with_backup(dataframe: pd.DataFrame, table_name: str, backup_table_name: str, database: str = "fantasy_football"):
	"""
	Appends a current MySQL table to another table (a backup), then overwrite the MySQL table with a dataframe.
	:param dataframe: A pandas dataframe
	:param table_name: Table name to write to
	:param backup_table_name: Backup table name to write to
	:param database: The database to write to
	:return: None
	"""
	backup = sql_read("SELECT * FROM {database}.{table_name}".format(database=database, table_name=table_name))
	sql_write(backup, backup_table_name, database, if_exists="append")
	sql_write(dataframe, table_name, database, if_exists="replace")
