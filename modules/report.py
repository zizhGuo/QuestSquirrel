import os
class ReportGenerator:
    def __init__(self, config, root_path, date):
        # self.strategy = self._create_strategy_instance(report_config)
        self.output_dir = config['output_dir']
        self.report_name = config['report_name']
        self.visual_name = config['visual_name']
        # 等价于email里的send_file_name
        self.final_visual_name = config['final_visual_name']
        self.root_path = root_path
        self.date = date # string yyyy-mm-dd

    def generate(self, df_write, sheet_name):
        # 生成报表
        print('df_write:')
        print(df_write)
        print('sheet_name:')
        print(sheet_name)

        from pandas import ExcelWriter
        try:
            output_file = os.path.join(self.root_path, self.output_dir)+'/'+self.report_name+'_'+self.date+'.xlsx'
            writer = ExcelWriter(output_file, engine='openpyxl')
            # Loop through the list and write each CSV to a different sheet in the same XLSX file
            for i, df in enumerate(df_write):
                # print(df)
                df.to_excel(writer, sheet_name=f'{sheet_name[i]}', index=False)
            # Save the XLSX file
            writer.close()
            print('Generate report successfully.')
        except Exception as e:
            print('Generate report failed.')
            print(e)
            return
    
    def generate_visual(self, visual_write):
        """ 
        # list of pages
        list of list of graphs
        """
        from pyecharts.charts import Page
        page = Page()
        output_file_copy = os.path.join(self.root_path, self.output_dir)+'/'+self.visual_name
        print(visual_write)
        for i, graph_list in enumerate(visual_write):
            if graph_list is None:
                continue
            for graph in graph_list:
                page.add(graph)
                page.render(output_file_copy+'_'+str(i)+'.html')
        final_output_file = os.path.join(self.root_path, self.output_dir)+'/'+self.final_visual_name+'_'+self.date+'.html'
        page.render(final_output_file)
