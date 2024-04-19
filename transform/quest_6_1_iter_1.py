from base_transform import BaseTransform

class Envir4(BaseTransform):
    """6-1-iter1-生态4表-去掉渔场id列"""
    def __init__(self):
        super(BaseTransform, self).__init__()
        
    @BaseTransform.drop_columns
    def prir_edit(self, *args):
        pass

    def run(self, ws, df, start_row):
        df = self.prir_edit(df, columns=['渔场id'])
        ret_row = super(Envir4, self).run(ws, df, start_row)
        return ret_row
