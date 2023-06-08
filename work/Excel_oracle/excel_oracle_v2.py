import os
import re
import calendar
import pandas as pd
from sqlalchemy import create_engine, exc, text
from datetime import datetime

# 创建数据库引擎
engine = create_engine('oracle+cx_oracle://system:123888Abc@localhost:1521/ORCL')

# 获取数据库连接
connection = engine.connect()

# 清空etl_log表
connection.execute(text("DELETE FROM etl_log"))
connection.commit()

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
                # 获取当前系统时间
                run_time = datetime.now().strftime('%Y.%m.%d %H:%M')

                # 将数据写入Oracle数据库
                try:
                    df.to_sql(sheet_name, engine, if_exists='append', index=False)
                    connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 1})
                except exc.SQLAlchemyError as e:
                    connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 0})

connection.commit() 
# 关闭数据库连接
connection.close()
