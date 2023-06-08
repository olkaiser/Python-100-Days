# 每个表的表结构需要根据sheet页的表头来定义。
# 	a.如何定义：读取sheet页时，直到读取到'asdfjkh'，则该行即为表头
# 	b.数据插入的内容则为该行以下部分


import os
import re
import calendar
import pandas as pd
from sqlalchemy import create_engine, exc, text, MetaData, Table, Column, String
from datetime import datetime

# 创建数据库引擎
engine = create_engine('oracle+cx_oracle://system:123888Abc@localhost:1521/ORCL')

# 获取数据库连接
connection = engine.connect()

# 清空etl_log表
connection.execute(text("DELETE FROM etl_log"))

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
                # 读取整个sheet页
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

                # 找到包含'数据自编码(asdfjkh)'的行
                header_row = df[df.apply(lambda row: row.astype(str).str.contains('asdfjkh').any(), axis=1)].index[0]

                # 将该行以下的所有行视为数据，该行视为表头
                df.columns = df.iloc[header_row]
                df = df.iloc[header_row + 1:]

                # 添加data_date和transaction_date列
                df['data_date'] = data_date
                df['transaction_date'] = transaction_date

                # 获取当前系统时间
                run_time = datetime.now().strftime('%Y%m%d%H%M%S')

                # 尝试将数据写入Oracle数据库
                try:
                    df.to_sql(sheet_name, engine, if_exists='append', index=False)
                except exc.SQLAlchemyError as e:
                    # 如果表不存在，创建表
                    if 'table or view does not exist' in str(e):
                        metadata = MetaData()
                        columns = [Column(re.sub(r'\W+', '', name), String(2000)) for name in df.columns]
                        table = Table(re.sub(r'\W+', '', sheet_name), metadata, *columns)
                        metadata.create_all(engine)

                        # 再次尝试将数据写入表
                        try:
                            df.to_sql(re.sub(r'\W+', '', sheet_name), engine, if_exists='append', index=False)
                        except exc.SQLAlchemyError as e:
                            print(f"Failed to write data to table {sheet_name}: {e}")

                # 将日志信息写入etl_log表
                try:
                    connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 1})
                except exc.SQLAlchemyError as e:
                    connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 0})

# 关闭数据库连接
connection.close()
