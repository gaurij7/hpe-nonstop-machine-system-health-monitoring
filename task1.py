import pandas as pd
import mysql.connector as MySqldb
from datetime import datetime

def update_csv_with_current_timestamp(file_paths):
  current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  for file_path in file_paths:
    df = pd.read_csv(file_path)
    if 'from_timestamp' in df.columns:
      df['from_timestamp'] = current_timestamp
      df['from_timestamp'] = df['from_timestamp'].astype(str)
    else:
      print(f"Warning: 'from_timestamp' column not found in {file_path}")
    df.to_csv(file_path, index = False)

def load_data_to_mysql(file_paths, table_names):
  try:
          db_connection = MySqldb.connect(
                    host='localhost',
                    port= 'xxxx',
                    database='OMNIDASH_TEST',
                    user='root',
                    password='root',
                    allow_local_infile=True)
          if db_connection.is_connected():
                print('Connected to MySQL database')
                cursor = db_connection.cursor()
                for i in range(len(file_paths)):
                    file_path = file_paths[i]
                    table_name = table_names[i]
                    load_query = f"""LOAD DATA LOCAL INFILE '{file_path}' 
                                     INTO TABLE {table_name} 
                                     FIELDS TERMINATED BY ',' 
                                     ENCLOSED BY '"'  
                                     ESCAPED BY '\\\\'
                    cursor.execute(load_query)
                    db_connection.commit()
                    print(f'Data loaded from {file_path}  into MySQL table {table_name}')
          else:
                print('Not connected to MySQL database')
  except Exception as e:
        print(f"Error: {str(e)}")
# Example usage:
if __name__ == '__main__':
  file_paths = ['/home/gauri/cpucsv_opca.csv', '/home/gauri/cpucsv_optx.csv', '/home/gauri/proccsv_opca.csv', '/home/gauri/proccsv_optx.csv', '/home/gauri/di
  table_names = ['cpucsv_opca', 'cpucsv_optx', 'proccsv_opca', 'proccsv_optx', 'disccsv_opca', 'disccsv_optx', 'tmfcsv_opca', 'tmfcsv_optx']
  #file_paths = ['/home/gauri/cpucsv_opca.csv', '/home/gauri/proccsv_opca.csv']
  #table_names  = ['cpucsv_opca', 'proccsv_opca']
  update_csv_with_current_timestamp(file_paths)
  load_data_to_mysql(file_paths, table_names)
