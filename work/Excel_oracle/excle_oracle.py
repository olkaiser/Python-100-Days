import os
import re
import calendar
import pandas as pd
from sqlalchemy import create_engine, exc, text, MetaData, Table, Column, String
from datetime import datetime
import configparser
import pysftp

def get_header_list(df, start_row):
    header_list = []
    columns_num = df.shape[1]
    for i in range(0, columns_num):
        first_not_null_value = df.loc[start_row:, i].dropna().iloc[0]
        header_list.append(first_not_null_value)
    return header_list

def process_excel_file(folder_path, engine):
    connection = engine.connect()

    for filename in os.listdir(folder_path):
        if filename.endswith('.xlsx') and re.search(r'\d{4}年\d{2}月', filename):
            year, month = map(int, re.findall(r'\d+', filename))
            data_date = f'{year}{month:02d}{calendar.monthrange(year, month)[1]}'
            transaction_date = f'{year}{month:02d}'

            excel_path = os.path.join(folder_path, filename)
            with pd.ExcelFile(excel_path) as xls:
                for sheet_name in xls.sheet_names:
                    # try:
                    #     connection.execute(text(f"DROP TABLE {sheet_name}"))
                    # except exc.SQLAlchemyError as e:
                    #     pass 
                    df = pd.read_excel(xls, sheet_name, header=None)
                    start_row = df[df.apply(lambda row: row.astype(str).str.contains('asdfjkh').any(), axis=1)].index[0]
                    header_list = get_header_list(df, start_row)
                    
                    iloc_key = max((df[df == value].dropna(how='all').index[0] for value in header_list))

                    df['data_date'] = data_date
                    df['transaction_date'] = transaction_date
                    run_time = datetime.now().strftime('%Y.%m.%d %H:%M')

                    header_list.extend(['data_date', 'transaction_date'])
                    header_list = [f'tmp_{i}' if col == '预留字段' else col for i, col in enumerate(header_list, 1)]
                    final_header = [re.sub(r'[^a-zA-Z0-9_]', '', f'tmp_{i}' if type(col) != type('str') else col) for i, col in enumerate(header_list, 1)]
                    
                    tmp_df = df.iloc[iloc_key+1:,:]
                    new_df = pd.DataFrame(tmp_df.values, columns=final_header)
                    # df = new_df
                    df = new_df.fillna(value='').astype(str)

                    try:
                        df.to_sql(sheet_name, engine, if_exists='append', index=False)
                    except exc.SQLAlchemyError as e:
                        if 'table or view does not exist' in str(e):
                            metadata = MetaData()
                            columns = [Column(name, String(2000)) for name in final_header]
                            table = Table(re.sub(r'\W+', '', sheet_name), metadata, *columns)
                            metadata.create_all(engine)
                            try:
                                df.to_sql(re.sub(r'\W+', '', sheet_name), engine, if_exists='append', index=False)
                            except exc.SQLAlchemyError as e:
                                print(f"Failed to write data to table {sheet_name}: {e}")

                    try:
                        connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 1})
                    except exc.SQLAlchemyError as e:
                        connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 0})
                        print(f"Failed to write log to table etl_log: {e}")
                        print('文件名：',filename,'sheet:',sheet_name,'日期：',data_date,'运行时间：',run_time)

                # connection.commit()

    connection.close()

# # Create engine
# engine = create_engine('oracle+cx_oracle://system:123888aBc@localhost:1521/ORCL')
# folder_path = r'C:\Users\45434\Desktop\FAL'

config = configparser.ConfigParser()
config.read('config.ini')  # 读取配置文件

# 1.本地
# # 创建数据库引擎
# engine = create_engine(config['DATABASE']['ENGINE'])
# # 指定文件夹路径
# folder_path = config['PATH']['FOLDER_PATH']

# 2.远程
# 获取数据库的配置信息
database_engine = config.get('DATABASE', 'ENGINE')

# 获取远程服务器的配置信息
remote_host = config.get('REMOTE_SERVER', 'HOST')
remote_username = config.get('REMOTE_SERVER', 'USERNAME')
remote_password = config.get('REMOTE_SERVER', 'PASSWORD')
remote_path = config.get('REMOTE_SERVER', 'PATH')

# 创建数据库引擎
engine = create_engine(database_engine)

# 关闭密钥！！！！！！！
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None    # disable host key checking.

# 使用pysftp连接远程服务器
with pysftp.Connection(host=remote_host, username=remote_username, password=remote_password, cnopts=cnopts) as sftp:
    
    # 创建一个新的本地目录来存储下载的文件
    local_folder = "local_folder"  
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    # 切换到远程目录
    remote_files = sftp.listdir(remote_path)
    for remote_file in remote_files:
        # 定义本地和远程的文件路径
        remote_file_path = os.path.join(remote_path, remote_file)
        local_file_path = os.path.join(local_folder, remote_file)

        # 下载文件到本地
        sftp.get(remote_file_path, local_file_path)

# Delete etl_log table
with engine.begin() as connection:
    connection.execute(text("DELETE FROM etl_log"))

# connection = engine.connect()
    for filename in os.listdir(local_folder):
        if filename.endswith('.xlsx'):
            excel_path = os.path.join(local_folder, filename)
            with pd.ExcelFile(excel_path) as xls:
                for sheet_name in xls.sheet_names:
                    try:
                        connection.execute(text(f"DROP TABLE {sheet_name}"))
                    except exc.SQLAlchemyError as e:
                        pass 

process_excel_file(local_folder, engine)
