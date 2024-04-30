from base_transform import BaseTransform

from utils_dateframe import drop_columns, select_columns
from utils_worksheet import df2ws, merge_cells_general, insert_row

class Envir4:
    """6-1-iter1-生态4表-去掉渔场id列
    steps:
    1. drop_columns
    2. df2ws
    """
    def __init__(self):
        self.drop_columns = ['渔场id']
        
    @drop_columns
    def prir_edit(self, *args, **kwargs):
        pass

    @df2ws
    def on_edit(self, *args, **kwargs):
        pass

    def process_v1(self, ws, df, start_row, *args, **kwargs):
        df = self.prir_edit(df, columns=self.drop_columns)
        return self.on_edit(ws, df, start_row)
    
    def run(self, ws, df, start_row):
        return self.process_v1(ws, df, start_row)

# class Envir4(BaseTransform):
#     """6-1-iter1-生态4表-去掉渔场id列"""
#     def __init__(self):
#         super(BaseTransform, self).__init__()
        
#     @BaseTransform.drop_columns
#     def prir_edit(self, *args):
#         pass

#     def run(self, ws, df, start_row):
#         df = self.prir_edit(df, columns=['渔场id'])
#         ret_row = super(Envir4, self).run(ws, df, start_row)
#         return ret_row
