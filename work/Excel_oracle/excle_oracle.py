import os
import re
import calendar
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine

# Oracle数据库配置
oracle_config = {
    'user': 'system',
    'password': '123888Abc',
    'dsn': cx_Oracle.makedsn('localhost', 1521, 'ORCL')
}

# 创建数据库引擎
engine = create_engine('oracle+cx_oracle://system:123888Abc@localhost:1521/ORCL')

# 连接Oracle数据库
connection = cx_Oracle.connect(**oracle_config)
cursor = connection.cursor()

# 遍历指定文件夹中的Excel文件
folder_path = 'C:\\Users\\45434\\Desktop\\FAL'
for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx') and re.search(r'\d{4}年\d{2}月', filename):
        # 从文件名中提取日期，并生成data_date和transaction_date
        year, month = map(int, re.findall(r'\d+', filename))
        data_date = f'{year}{month:02d}{calendar.monthrange(year, month)[1]}'
        transaction_date = f'{year}{month:02d}'

        # 读取Excel文件中的每个sheet页，并将数据写入Oracle数据库
        excel_path = os.path.join(folder_path, filename)
        with pd.ExcelFile(excel_path) as xls:
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                df['data_date'] = data_date
                df['transaction_date'] = transaction_date

                # 将数据写入Oracle数据库
                try:
                    df.to_sql(sheet_name, engine, if_exists='append', index=False)  # 使用engine而不是connection
                    cursor.execute("INSERT INTO etl_log (table_name, sheet_name, data_date, status) VALUES (:1, :2, :3, :4)", (filename, sheet_name, data_date, 1))
                except Exception as e:
                    cursor.execute("INSERT INTO etl_log (table_name, sheet_name, data_date, status) VALUES (:1, :2, :3, :4)", (filename, sheet_name, data_date, 0))

# 提交事务，关闭连接
connection.commit()
cursor.close()
connection.close()
