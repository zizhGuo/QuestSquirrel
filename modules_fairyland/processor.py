import os
import pandas as pd

class BaseStrategy:
    def process(self, df:pd.DataFrame):
        """
        处理数据的方法，需要在子类中具体实现。
        :param data: 需要处理的数据
        :return: 处理后的数据
        """
        raise NotImplementedError("Process method must be defined in subclass")
    
class Strategy_2_2_sheet_1(BaseStrategy):
    def process(self, df):
        """
        df format:
        createdate,realmoney,playerid,vip
        2023-12-06,50.0,10187582,12
        2023-12-06,6.0,1812322,12
        2023-12-06,98.0,7077023,12
        2023-12-06,30.0,15248228,10
        2023-12-06,50.0,10787786,9
        2023-12-06,18.0,15248228,10
        """
        # group by createdate, vip and realmoney, calculate number of distinct playerid
        # row: createdate, vip, number of distinct playerid
        # column: realmoney = 6, 18, 30, 50, 98, ...
        # value: number of distinct playerid
        df = df.rename(columns={'createdate':'日期', 'realmoney':'充值档位', 'playerid':'玩家ID', 'vip':'VIP等级'})
        df = df.astype({'充值档位':int})
        df = df.groupby(['日期','VIP等级','充值档位']).agg({'玩家ID':'nunique'}).reset_index()
        # df = df.rename(columns={'playerid':'人数'})
        df = df.pivot(index=['日期','VIP等级'], columns='充值档位', values='玩家ID')
        df = df.fillna(0)
        df = df.astype(int)
        df = df.reset_index()
        df.columns = [str(col) + '元' if type(col) == int else col for col in df.columns.values]
        # df = df.rename(columns={'vip':'VIP等级'})
        print(df)        
        return df
    
class Strategy_2_2_sheet_2(BaseStrategy):
    def process(self, df):
        """
        df format:
        createdate,realmoney,playerid,vip
        2023-12-06,50.0,10187582,12
        2023-12-06,6.0,1812322,12
        2023-12-06,98.0,7077023,12
        2023-12-06,30.0,15248228,10
        2023-12-06,50.0,10787786,9
        2023-12-06,18.0,15248228,10
        """

        """
        output format:
        column
        createdate, vip, 6_sales, 6_sum_realmoeny, 
        """
        # modify column name as 日期, 充值金额, 玩家ID, VIP等级
        df = df.rename(columns={'createdate':'日期', 'realmoney':'充值档位', 'playerid':'玩家ID', 'vip':'VIP等级'})

        # convert columns realmoney to int
        df = df.astype({'充值档位':int})

        grouped = df.groupby(['日期', 'VIP等级', '充值档位'])

        # 计算各组的订单数和充值总额
        aggregated = grouped.agg(sales=('充值档位', 'size'), total_realmoney=('充值档位', 'sum'))

        # 计算充值占比
        total_sales_per_date_realmoney = df.groupby(['日期', '充值档位'])['充值档位'].count()
        # aggregated['ratio'] = aggregated['sales'] / aggregated.index.get_level_values('realmoney').map(total_sales_per_realmoney)
        aggregated['ratio'] = aggregated.apply(
            lambda x: x['sales'] / total_sales_per_date_realmoney.loc[(x.name[0], x.name[2])], axis=1
        )

        # 重设索引以便于输出
        result = aggregated.reset_index().sort_values(by=['日期', '充值档位', 'VIP等级'])
        # 重命名列
        result = result.rename(columns={'sales':'订单数', 'total_realmoney':'充值金额', 'ratio':'订单占比'})

        # 使用pivot_table重构DataFrame
        pivot_df = result.pivot_table(
            index=['日期', 'VIP等级'],
            columns='充值档位',
            values=['订单数', '充值金额', '订单占比']
        )

        pivot_df = pivot_df.fillna(0)
        def process_column(col):
            if col[0] == '订单占比':
                pivot_df[col] = (pivot_df[col] * 100).round(1).astype(str) + '%'
            elif col[0] == '充值金额':
                pivot_df[col] = pivot_df[col].astype(int)
            else:
                pivot_df[col] = pivot_df[col].astype(int)

        pivot_df.apply(lambda col: process_column(col.name), axis=0)
            # if '订单占比' in col:

        # 重命名列
        pivot_df.columns = ['-'.join([str(col[1])+"元", str(col[0])]) for col in pivot_df.columns.values]
        print(pivot_df.columns)

        def sort_columns(df):
            # 解析列名并提取排序依据
            def parse_column(col):
                _realmoney, metric = col.split('-')
                realmoney = _realmoney.split('元')[0]
                metric_order = {'订单数': 1, '充值金额': 2, '订单占比': 3}
                return (int(realmoney), metric_order.get(metric, 0))

            # 对列名进行排序
            sorted_columns = sorted(df.columns, key=parse_column)
            print(sorted_columns)

            # 根据排序后的列名重排DataFr
            return df[sorted_columns]
        
        # 应用函数排序pivot_df
        sorted_pivot_df = sort_columns(pivot_df)
        # 重设索引，以便打印和查看
        sorted_pivot_df.reset_index(inplace=True)

        # 输出结果
        print(sorted_pivot_df)

        # df = df.astype(int)
        # df = df.reset_index()

        return sorted_pivot_df
    

class Strategy_2_3(BaseStrategy):
    def process(self, df):
        return df
    
class StrategyA(BaseStrategy):
    # data as pandas dataframe
    def process(self, df):
        # select rows where column 总充值人数 > 10000
        # df = df[df['总充值人数'] > 10000]
        return df

class StrategyB(BaseStrategy):
    def process(self, df):
        # 特定的处理逻辑B
        pass

class DataProcessor:
    def __init__(self, config, root_path):
        self.config = config
        self.root_path = root_path
        self.strategy = self._create_strategy_instance(config['strategy_name'])
        self.read_from_file = config['read_from_file']
        self.save_to_file = config['save_to_file']
        self.temp_save_path = config['temp_save_path']
        self.read_file_name = config['read_file_name']
        self.save_file_name = config['save_file_name']

    def _create_strategy_instance(self, strategy_name):
        # 使用 globals() 或者专门的策略注册字典来查找对应的类
        if strategy_name in globals():
            strategy_class = globals()[strategy_name]
            if issubclass(strategy_class, BaseStrategy):
                return strategy_class()
        raise ValueError(f"Unknown strategy: {strategy_name}")

    def _save_data(self, df):
        file = os.path.join(self.root_path, self.temp_save_path)+'/'+self.save_file_name
        df.to_csv(file, index=False)

    def _read_date(self):
        file = os.path.join(self.root_path, self.temp_save_path)+'/'+self.read_file_name
        print('file: {}'.format(file))
        df = pd.read_csv(file,header=0,encoding='utf-8')
        print(df.head(10))
        return df

    def process(self, df):
        try:
            if self.read_from_file:
                print("read from file")
                df = self._read_date()
                print("self.config['temp_save_path']: {}".format(self.temp_save_path))
            df = self.strategy.process(df)
            if self.save_to_file:
                self._save_data(df)
        except Exception as e:
            print('Process failed.')
            if not self.read_from_file:
                print('Dataprocessing: 读取flag为false，但是读取数据失败，可能是数据文件不存在。')
            print(e)
            return
        return df


