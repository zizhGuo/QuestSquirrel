from base_transform import BaseTransform

class BreakthruLevelNonOperate(BaseTransform):
    """6-1-iter1-生态4表-去掉渔场id列"""
    def __init__(self):
        super(BaseTransform, self).__init__()
        
    @BaseTransform.drop_columns
    def prir_edit(self, *args):
        pass

    def run(self, ws, df, start_row):
        df = self.prir_edit(df, columns=['等级'])
        ret_row = super(BreakthruLevelNonOperate, self).run(ws, df, start_row)
        return ret_row

class LevelFailureTransform(BaseTransform):
    def __init__(self):
        super(BaseTransform, self).__init__()

    @BaseTransform.merge_cells
    def post_edit(self, *args):
        pass

    def run(self, ws, df, start_row, dt = '日期', level = '等级名称', target_column = '玩家名单'):
        ret_row = super(LevelFailureTransform, self).run(ws, df, start_row)
        self.post_edit(ws, df, start_row, dt= dt, level= level, target_column= target_column)
        return ret_row