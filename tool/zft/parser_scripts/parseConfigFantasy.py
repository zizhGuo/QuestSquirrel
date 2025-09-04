"""仙魔渔场计费点

字段说明：
一级功能名、二级功能名、商品ID、商品名
"""
import os
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='version arg')
parser.add_argument('--version', type=str, help='version arg')
parser.add_argument('--customized', type=str, help='version arg')
args = parser.parse_args()

CUSTOMIZED = args.customized
CUR_VERSION = args.version
# CUR_VERSION = 'previous'
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARRENT_FILE_DIR = os.path.dirname(CURRENT_FILE_DIR)
excel_dir = os.path.join(PARRENT_FILE_DIR, 'config_xlsx', CUSTOMIZED)
sql_dir = os.path.join(PARRENT_FILE_DIR, 'config_sql', CUR_VERSION)

config_file = '_config_fantasy.xlsx'
sheet_name = 'Sheet1'
MY_TABLE_NAME = 'guozizhun.config_fantasy'
sql_file1 = '_config_fantasy_1_drop.sql'
sql_file2 = '_config_fantasy_2_create.sql'
sql_file3 = '_config_fantasy_3_insert.sql'

excel_file = os.path.join(excel_dir, config_file)
sql_file1 = os.path.join(sql_dir, sql_file1)
sql_file2 = os.path.join(sql_dir, sql_file2)
sql_file3 = os.path.join(sql_dir, sql_file3)
all_file = os.path.join(sql_dir, 'all.sql')

def read_excel_generate_sql(excel_file, sheet_name, columns):
    sql1 = f"""
drop table if exists {MY_TABLE_NAME}
"""
    sql2 = f"""
create table {MY_TABLE_NAME} (
func_name string,
sub_func_name string,
goodsid int,
goodsname string
)
row format delimited
fields terminated by '|'
lines terminated by '\\n'
stored as textfile
"""
    sql = ''
    # Read the Excel file
    df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl')

    # Extract the specified columns and drop rows with NaN in these columns
    # df = df.iloc[:, columns].dropna(how='any')
    df = df.iloc[:, columns]
    # re index the row
    df.columns = ['func_name','sub_func_name', 'goodsid', 'goodsname']
    # drop rows if id is non numeric
    df = df[df['goodsid'].apply(lambda x: str(x).isnumeric())]
    df = df.reset_index(drop=True)
    # print(df)

    # Prepare SQL statements
    sql_statements = []
    sql += f"INSERT INTO {MY_TABLE_NAME} (func_name, sub_func_name, goodsid, goodsname) VALUES\n"
    for index, row in df.iterrows():
        func_name, sub_func_name, goodsid, goodsname = row

        if index != 0:
            # Format the SQL insert statement
            sql += f",('{func_name}', '{sub_func_name}', {goodsid}, '{goodsname}')\n"
        else:
            sql += f"('{func_name}', '{sub_func_name}', {goodsid}, '{goodsname}')\n"
        # sql_statements.append(sql)

    # return sql_statements
    return sql1, sql2, sql



columns = [0, 1, 2, 3]  # Column indices for 'id', 'name', and 'functionId1'
# Generate SQL statements
sql1, sql2, sql3 = read_excel_generate_sql(excel_file, sheet_name, columns)
sql_all = sql1 + ';\n' + sql2 + ';\n' + sql3 + ';\n'

# print(sql1)
# print(sql2)
# print(sql3)

with open(sql_file1, 'w', encoding='UTF-8') as f:
    f.write(sql1)
with open(sql_file2, 'w', encoding='UTF-8') as f:
    f.write(sql2)
with open(sql_file3, 'w', encoding='UTF-8') as f:
    f.write(sql3)

if not os.path.exists(all_file):
    os.makedirs(os.path.dirname(all_file), exist_ok=True)  # 确保目录存在
    with open(all_file, 'w', encoding='UTF-8') as f:
        f.write(sql_all)  # 如果文件不存在，则创建并写入
else:
    with open(all_file, 'r+', encoding='UTF-8') as f:
        content = f.read()
        if content:  # 如果文件存在且不为空，则先换行
            f.write('\n')
        f.write(sql_all)  # 追加内容

