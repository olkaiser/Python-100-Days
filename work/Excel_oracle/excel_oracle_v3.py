
import os
import re
import calendar
import pandas as pd
from sqlalchemy import create_engine, exc, text, MetaData, Table, Column, String
from datetime import datetime

# # 创建数据库引擎
# engine = create_engine('oracle+cx_oracle://system:123888Abc@localhost:1521/ORCL')

# # 获取数据库连接
# connection = engine.connect()

# # 清空etl_log表
# connection.execute(text("DELETE FROM etl_log"))
# connection.commit() 

# # 遍历指定文件夹中的Excel文件
# folder_path = 'C:\\Users\\45434\\Desktop\\FAL'
# for filename in os.listdir(folder_path):
#     if filename.endswith('.xlsx') and re.search(r'\d{4}年\d{2}月', filename):
#         # 从文件名中提取日期，并生成data_date和transaction_date
#         year, month = map(int, re.findall(r'\d+', filename))
#         data_date = f'{year}{month:02d}{calendar.monthrange(year, month)[1]}'
#         transaction_date = f'{year}{month:02d}'

#         # 读取Excel文件中的每个sheet页，并将数据写入Oracle数据库
#         excel_path = os.path.join(folder_path, filename)
#         with pd.ExcelFile(excel_path) as xls:
#             for sheet_name in xls.sheet_names:
#                 # 读取整个sheet页
#                 df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

#                 # 找到包含'asdfjkh'的行
#                 header_row = df[df.apply(lambda row: row.astype(str).str.contains('asdfjkh').any(), axis=1)].index[0]

#                 # 将该行以下的所有行视为数据，该行视为表头
#                 tmp_header = df.iloc[header_row]
#                 # 存放临时行
#                 tmp_row = df.iloc[header_row - 1]
#                 df = df.iloc[header_row + 1:]

#                 # 处理临时表头                
#                 tmp_header = [re.sub(r'[^a-zA-Z0-9_]', '', f'tmp_{i}' if type(col) != type('str') else col) for i, col in enumerate(tmp_header, 1)]


# import os
# import pandas as pd
# import calendar
# import re

# 创建数据库引擎
engine = create_engine('oracle+cx_oracle://system:123888aBc@localhost:1521/ORCL')

# 获取数据库连接
connection = engine.connect()

# 清空etl_log表
connection.execute(text("DELETE FROM etl_log"))
connection.commit() 

# 指定文件夹路径
folder_path = r'C:\Users\45434\Desktop\FAL'

# 定义表头
# header_list=['asdfjkh','必选(sasdf0)','阿斯蒂芬啊(asdfjkh2)','阿斯蒂芬啊(asdfjkh3)','阿斯蒂芬啊(asdfjkh4)','阿斯蒂芬啊(asdfjkh5)','阿斯蒂芬啊(asdfjkh6)','我碟(next)','阿斯蒂芬啊(asdfjkh8)','阿斯蒂芬啊(asdfjkh9)','阿斯蒂芬啊(asdfjkh10)','asdf','阿斯蒂芬啊(asdfjkh12)']

# 遍历指定文件夹中的Excel文件
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
                df = pd.read_excel(xls, sheet_name='z01', header=None)

                start_row = header_row = df[df.apply(lambda row: row.astype(str).str.contains('asdfjkh').any(), axis=1)].index[0]
                
                header_list = []
                columns_num = df.shape[1]

                for i in range(0, columns_num):
                    first_not_null_value = df.loc[start_row:, i].dropna().iloc[0]
                    header_list.append(first_not_null_value)


                # 找到包含header_list中元素的最大行索引
                iloc_key = max((df[df == value].dropna(how='all').index[0] for value in header_list))

            
                # 添加data_date和transaction_date列
                df['data_date'] = data_date
                df['transaction_date'] = transaction_date

                # 获取当前系统时间
                run_time = datetime.now().strftime('%Y.%m.%d %H:%M')

                # 添加data_date和transaction_date列
                header_list.extend(['data_date', 'transaction_date'])

                header_list = [f'tmp_{i}' if col == '预留字段' else col for i, col in enumerate(header_list, 1)]

                final_header = [re.sub(r'[^a-zA-Z0-9_]', '', f'tmp_{i}' if type(col) != type('str') else col) for i, col in enumerate(header_list, 1)]
                

                # 生成新的数据帧
                tmp_df = df.iloc[iloc_key+1:,:]
                new_df = pd.DataFrame(tmp_df.values, columns=final_header)
            # print(new_df)
                df = new_df

                # 尝试将数据写入Oracle数据库
                try:
                    df.to_sql(sheet_name, engine, if_exists='append', index=False)
                except exc.SQLAlchemyError as e:
                    # 如果表不存在，创建表
                    if 'table or view does not exist' in str(e):
                        metadata = MetaData()
                        columns = [Column(name, String(2000)) for name in final_header]
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
                    print(f"Failed to write log to table etl_log: {e}")
                    print('文件名：',filename,'sheet:',sheet_name,'日期：',data_date,'运行时间：',run_time)
            connection.commit() 

# connection.commit() 
# 关闭数据库连接
connection.close()















#                 # if header_row > 0:
#                 #     tmp_row = df.iloc[header_row - 1]
#                 # else:
#                 #     tmp_row = 2  # 或者你可以选择一个合适的默认值

#                 # 处理临时行，去除空值，只保留有效字符
#                 # tmp_row = [re.sub(r'\W+', '', str(col)) if pd.notnull(col) and str(col).strip() != '' else '' for col in tmp_row]
#                 tmp_row = [re.sub(r'[^a-zA-Z0-9_]', '', str(col)) if pd.notnull(col) and str(col).strip() != '' else '' for col in tmp_row]
#                 # 删除空字符串
#                 tmp_row = [col for col in tmp_row if col != '']

#                 # 判断处理完成的临时表头是否有空值
#                 if any('tmp_' in col for col in tmp_header):
#                     # 用处理完成的临时行依次填充，并生成一个新的表头
#                     tmp_count = 0
#                     final_header = []
#                     for col in tmp_header:
#                         if 'tmp_' in col:
#                             final_header.append(tmp_row[tmp_count])
#                             tmp_count += 1
#                         else:
#                             final_header.append(col)
#                 else:
#                     # 用处理完成的临时表头作为数据库表的表结构
#                     final_header = tmp_header

#                 # 将所有列转换为字符串类型
#                 df = df.fillna(value='').astype(str)

#                 # 添加data_date和transaction_date列
#                 df['data_date'] = data_date
#                 df['transaction_date'] = transaction_date
#                 # 获取当前系统时间
#                 run_time = datetime.now().strftime('%Y.%m.%d %H:%M')

#                 # 添加data_date和transaction_date列
#                 final_header.extend(['data_date', 'transaction_date'])

#                 # 删除数据框中的超出列名个数的列
#                 # df = df.iloc[:, :len(final_header)+2]

#                 # 将数据框的列名设置为final_header
#                 df.columns = final_header

#                 # 尝试将数据写入Oracle数据库
#                 try:
#                     df.to_sql(sheet_name, engine, if_exists='append', index=False)
#                 except exc.SQLAlchemyError as e:
#                     # 如果表不存在，创建表
#                     if 'table or view does not exist' in str(e):
#                         metadata = MetaData()
#                         columns = [Column(name, String(2000)) for name in final_header]
#                         table = Table(re.sub(r'\W+', '', sheet_name), metadata, *columns)
#                         metadata.create_all(engine)
#                     # 再次尝试将数据写入表
#                         try:
#                             df.to_sql(re.sub(r'\W+', '', sheet_name), engine, if_exists='append', index=False)
#                         except exc.SQLAlchemyError as e:
#                             print(f"Failed to write data to table {sheet_name}: {e}")

#                 # 将日志信息写入etl_log表
#                 try:
#                     connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 1})
#                 except exc.SQLAlchemyError as e:
#                     connection.execute(text("INSERT INTO etl_log (table_name, sheet_name, data_date, run_time, status) VALUES (:1, :2, :3, :4, :5)"), {'1': filename, '2': sheet_name, '3': data_date, '4': run_time, '5': 0})
#                     print(f"Failed to write log to table etl_log: {e}")
#                     print('文件名：',filename,'sheet:',sheet_name,'日期：',data_date,'运行时间：',run_time)
#             connection.commit() 

# # connection.commit() 
# # 关闭数据库连接
# connection.close()
