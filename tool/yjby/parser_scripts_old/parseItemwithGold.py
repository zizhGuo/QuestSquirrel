
import os
CURRENT_FILE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import argparse
parser = argparse.ArgumentParser(description='version arg')
parser.add_argument('--version', type=str, help='version arg')
args = parser.parse_args()
CUR_VERSION = args.version

MY_TABLE_NAME = 'guozizhun.config_item_gold_3d'
import pandas as pd

def read_excel_generate_sql(excel_file, sheet_name, columns):
    sql = f"""
drop table if exists {MY_TABLE_NAME};
create table {MY_TABLE_NAME} (
itemid int,
itemtype string,
name string,
gold int
)
row format delimited
fields terminated by '|'
lines terminated by '\\n'
stored as textfile
;
"""
    sql += '\n'
    # Read the Excel file
    df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl')

    # Extract the specified columns and drop rows with NaN in these columns
    # df = df.iloc[:, columns].dropna(how='any')
    df = df.iloc[:, columns]
    # re index the row
    df.columns = ['itemid','itemtype', 'name', 'gold']
    # drop rows if id is non numeric
    df = df[df['itemid'].apply(lambda x: str(x).isnumeric())]
    df = df.reset_index(drop=True)
    print(df)

    # Prepare SQL statements
    sql_statements = []
    sql += f"INSERT INTO {MY_TABLE_NAME} (itemid, itemtype, name, gold) VALUES\n"
    for index, row in df.iterrows():
        itemid, itemtype, name, gold = row
        # if level == '1':
        #     continue
        if str(gold).isnumeric() == False:
            continue
        if index != 0:
            # Format the SQL insert statement
            sql += f",({itemid}, '{itemtype}', '{name}', {gold})\n"
        else:
            sql += f"({itemid}, '{itemtype}', '{name}', {gold})\n"
        # sql_statements.append(sql)

    sql += ';'
    # return sql_statements
    return sql

# Replace 'your_table_name' with the actual table name
# excel_file = '{}/config_shop_test.xlsx'.format(CURRENT_FILE_DIR)
excel_file = '{}/config_xlsx/{}/config_item.xlsx'.format(CURRENT_FILE_DIR, CUR_VERSION)
sheet_name = 'config_item'
columns = [1, 3, 6, 44]  # Column indices for 'id', 'name', and 'functionId1'

# Generate SQL statements
sql_code = read_excel_generate_sql(excel_file, sheet_name, columns)

# Print SQL statements
# for sql in sql_code:
#     print(sql)
print(sql_code)

# Write sql_code to a file named 'config_goodsid_push_function_id_schema1.sql'
with open('{}/config_sql/{}/_config_item_gold.sql'.format(CURRENT_FILE_DIR, CUR_VERSION), 'w', encoding='UTF-8') as f:
    f.write(sql_code)

file_path = os.path.join(CURRENT_FILE_DIR, 'config_sql', CUR_VERSION, 'all.sql')

if not os.path.exists(file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # 确保目录存在
    with open(file_path, 'w', encoding='UTF-8') as f:
        f.write(sql_code)  # 如果文件不存在，则创建并写入
else:
    with open(file_path, 'r+', encoding='UTF-8') as f:
        content = f.read()
        if content:  # 如果文件存在且不为空，则先换行
            f.write('\n')
        f.write(sql_code)  # 追加内容