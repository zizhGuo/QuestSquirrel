import os
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, Border, Side, Alignment

'''
query:
  tasks_num: 8
  template_names: ['dws_mall_task1', 'dws_mall_task2' ,'dws_mall_task3' ,'dws_mall_task4', 'dws_envir_task1', 'dws_envir_task2', 'dws_envir_task3', 'dws_envir_task4']
  table_format: ['xlsx', 'xlsx', 'xlsx', 'xlsx', 'xlsx', 'xlsx', 'xlsx', 'xlsx']
  tables: ['dws_mall_task1.xlsx', 'dws_mall_task2.xlsx', 'dws_mall_task3.xlsx', 'dws_mall_task4.xlsx', 'dws_envir_task1.xlsx', 'dws_envir_task2.xlsx', 'dws_envir_task3.xlsx', 'dws_envir_task4.xlsx']
  info:
    req_cat: "request_5_1"
    req_iter: "iter_1"
    req_status: "deliver"
  params:
    NA: "NA"

excel:
  output_file: 'output.xlsx'
  worksheet: ['ws1', 'ws2', 'ws3', 'ws4', 'ws5', 'ws6', 'ws7', 'ws7']
  ws2title:
    ws1: '仙魔大陆各模块数据大盘'
    ws2: '仙魔大陆各礼包模块'
    ws3: '仙魔各礼包ID TOP10礼包'
    ws4: '仙魔各模块中礼包ID TOP10礼包'
    ws5: '每日活跃人数中各境界分布情况'
    ws6: '境界突破情况分布'
    ws7: '仙魔大陆渔场消耗'
  column: ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8']
  col2names:
    col1: ['日期',	'模块名称',	'点券兑换订单数',	'点券兑换金额',	'仙魔大陆整体订单占比',	'仙魔大陆整体金额占比',	'与前一日兑换订单数对比',	'与前一日点券兑换金额对比',	'与前一日订单数环比',	'与前一日兑换金额环比']
    col2: ['日期',	'模块名称',	礼包类型',	'兑换订单数',	'兑换金额',	'模块订单占比',	'模块兑换金额占比',	'与前一日兑换订单数对比',	'与前一日点券兑换金额对比',	'与前一日订单数环比',	'与前一日兑换金额环比']
    col3: ['日期'	,'序号'	,'礼包名称'	,'礼包类型'	,'礼包单价'	,'兑换订单数'	,'兑换金额'	,'仙魔大陆总兑换订单占比'	,'仙魔大陆总兑换金额占比'	,'与前一日兑换订单数对比'	,'与前一日点券兑换金额对比'	,'与前一日订单数环比'	,'与前一日兑换金额环比']
    col4: ['日期'	,'模块名称'	,'序号'	,'礼包名称'	,'礼包单价'	,'兑换订单数'	,'兑换金额'	,'仙魔大陆总兑换订单占比'	,'仙魔大陆总兑换金额占比'	,'与前一日兑换订单数对比'	,'与前一日点券兑换金额对比'	,'与前一日订单数环比'	,'与前一日兑换金额环比']
    col5: ['日期',	'境界等级',	'境界人数',	'各境界人数与前一日对比']
    col6: ['日期',	'境界等级',	'经历过对应境界等级总人数',	'类型', '操作总次数', '操作总人数']
    col7: [,'日期'	,'总游戏人数'	,'人均游戏时长（分钟）'	,'与前一日对比'	,'总消耗金币'	,'与前一日对比'	,'净分']
    col8: ['日期'	,'渔场名称'	,'渔场游戏人数'	,'人均游戏时长（分钟）'	,'与前一日对比'	,'总消耗金币'	,'消耗金币占比'	,'与前一日对比'	,'净分']
  style: ['general', 'general', 'general', 'general', 'general', 'general', 'general', 'general']
  style_setting:
    general:
      header: 
        font: 'Bold'
        border: 'Medium'
        align: 'Center'
      cell:
        font: 'Regular'
        border: 'Thin'
        align: 'Center'
'''
THICK_BORDER = Border(
    left=Side(style='thin'), 
    right=Side(style='thin'),
    top=Side(style='thin'), 
    bottom=Side(style='thin')
)
def adjust_column_width(ws):
    # 计算并调整每列宽度
    column_widths = {}
    for row in ws.iter_rows():
        for cell in row:
            if cell.value:
                # 计算当前单元格内容的长度
                cell_length = len(str(cell.value))
                # 如果是新列，或者当前单元格内容长度超过已记录的最大长度，则更新
                if cell.column_letter not in column_widths or cell_length > column_widths[cell.column_letter]:
                    column_widths[cell.column_letter] = cell_length
    # 设置列宽（略大于最大长度以避免内容截断）
    for col_letter, width in column_widths.items():
        ws.column_dimensions[col_letter].width = width*2  # 可根据需要调整额外宽度

def add_dataframe_to_worksheet(ws, df, start_row):
    current_row = start_row 
    for _, r in enumerate(dataframe_to_rows(df, index=False, header=True)):
        ws.append(r)
        # print(i)
        # print(ws[i])
        if current_row == start_row:
            for cell in ws[current_row]:
                cell.font = Font(bold=True)
                cell.border = THICK_BORDER
        current_row += 1
    # 在不同DataFrame之间添加空行作为分隔
    ws.append([])
    return current_row + 1
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)*2.2
        # print(f"length: {length}")
        ws.column_dimensions[column_cells[0].column_letter].width = length

class ReportSchedular:
    def __init__(self, config, root_path, module) -> None:
        self.report_generator = []
        for sub_report, _sub_config in config[module.report_module].items():
            _ = ReportGenerator(config, root_path, module, sub_report)
            self.report_generator.append(_)
    def run(self):
        for i in range(len(self.report_generator)):
            print('run report generator')
            try:
                print(f'generating report: {i}')
                self.report_generator[i].run()
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
            except Exception as e:
                print(f'gen report run failed. {i}')
                print(e)

class ReportGenerator:
    def __init__(self, config, root_path, module, sub_report):
        self.module = module
        self.tables = config[module.report_module][sub_report]['source_tables']
        # filter out the 'na' tables
        # self.tables = [table for table in self.tables if table != 'na']
        self.num_tables = len(self.tables)
        self.output_dir =  config[module.report_module][sub_report]['output_file']
        self.worksheet =  config[module.report_module][sub_report]['worksheet']
        self.ws2title =  config[module.report_module][sub_report]['ws2title']
        self.column =  config[module.report_module][sub_report]['column']
        self.col2names =  config[module.report_module][sub_report]['col2names']
        self.style =  config[module.report_module][sub_report]['style']
        # self.style_setting =  config[module.report_module][sub_report]['style_setting']
        self.root_path = root_path
        self.temp_save_path = config[module.report_module][sub_report]['temp_save_path']
        self.end_dt = config['end_dt']
 
    def run(self):
        wb = Workbook()
        wb.remove(wb.active)  # 移除初始的空worksheet
        ws_data = {}
        for table, ws in zip(self.tables, self.worksheet):
            if ws not in ws_data:
                ws_data[ws] = []
            ws_data[ws].append(table)
        print(f"ws_data: {ws_data}")
        for ws, files in ws_data.items():
            ws = wb.create_sheet(title=self.ws2title[ws])
            start_row = 1
            for file in files:
                _dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt, file)
                if not os.path.exists(_dir):
                    print(f"file {file} not existed")
                    continue
                df = pd.read_excel(_dir)
                # rename columns
                df.columns = self.col2names[self.column[self.tables.index(file)]]
                start_row = add_dataframe_to_worksheet(ws, df, start_row)

            # adjust_column_width(ws)

            # print(f'ws.columns:{ws.columns}')
            # for column_cells in ws.columns:
            #     length = max(len(str(cell.value)) for cell in column_cells)
            #     print(f"length: {length}")
            #     ws.column_dimensions[column_cells[0].column_letter].width = length

        # ws = your current worksheet
        # dims = {}
        # for row in ws.rows:
        #     for cell in row:
        #         if cell.value:
        #             dims[cell.column] = max((dims.get(cell.column, 0), len(str(cell.value))))    
        # for col, value in dims.items():
        #     ws.column_dimensions[col].width = value

        dir_path = os.path.join(self.root_path, self.temp_save_path, self.end_dt)
        file_path = os.path.join(dir_path, self.output_dir)
        print(f"file_path: {file_path}")
        wb.save(f"{file_path}")
        

    
    def generate_god_batch(self, df_write, sheet_name, mapper, write_xlsx):
        tables = {}
        for sheet_df in zip(sheet_name, df_write):
            if sheet_df[0] == 'na':
                continue
            if not tables.get(sheet_df[0]):
                tables[sheet_df[0]] = [sheet_df[1]]
            else:
                tables[sheet_df[0]].append(sheet_df[1])
        assert mapper.get('files2sheets') is not None, 'files2sheets is not existed'
        for file, sheet_list in mapper['files2sheets'].items():
            tables_new = {}
            for sheet in sheet_list:
                tables_new[sheet] = tables[sheet]
            write_xlsx(tables_new, self.root_path, self.output_dir, file)

