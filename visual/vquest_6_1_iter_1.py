"""
此模块用于实现任务5_1的第一次迭代的商城图示
"""
from visual.vquest_5_1_iter_1 import FairydemonMall

class PacificMall(FairydemonMall):
    def __init__(self, args):
        super().__init__(args)
        self.name = 'Pacific'
        self.para = {
            'title': '姚记捕鱼-环太平洋',
            '境界推送礼包': '境界推送礼包',
            '贺岁礼包': '贺岁礼包'
        }
        self.params.update(self.para)
    
    def plot_v1(self):
        return super().plot_v1()
    
    def plot(self):
        return self.plot_v1()