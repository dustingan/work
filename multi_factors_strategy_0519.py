import pandas as pd
import json, os
import utils.okx.Account_api as Account
import utils.okx.Trade_api as Trade
import utils.okx.Market_api as Market
from utils.orders_0514 import Order
from data_process.multi_factors_process import add_features
import utils.okx.Public_api as Public
pd.set_option("display.max_columns", None)

#多因子对冲策略
class Multi_Factors_Strategy():
    def __init__(self, invalid_symbols, okex_params, dir_path, macp_path, market='MARGIN'):
        #获取基本的数据
        self.kline_df = self.get_all_datas(dir_path) #获取所有的kline
        # self.kline_df = self.kline_df[self.kline_df['tic'].isin(vailid_symbols)]
        self.macp_df = self.get_all_datas(macp_path) #获取所有的macp
        self.market = market
        #此处为特殊情况，由于SWAP采用特殊符号，需要将macp或其他的来源符号进行修正
        if market == "SWAP":
            self.macp_df['tic'] = self.macp_df['tic'].apply(lambda x: x+'SWAP')
        #初始化okex_api
        self.accountAPI = Account.AccountAPI(**okex_params)
        self.marketAPI = Market.MarketAPI(**okex_params)
        self.tradeAPI = Trade.TradeAPI(**okex_params)
        #初始化订单类，处理订单与交易所的交互
        self.order_funs = Order(okex_params=okex_params, market=market)
        #删除不用的tics
        self.tics_info = self.get_tics_info(okex_params=okex_params, market=market)
        self.kline_df = self.kline_df.merge(self.tics_info, on="tic")
        self.kline_df = self.kline_df[~self.kline_df['tic'].isin(invalid_symbols)]
        # print("Account_Status:", self.accountAPI.get_account_config())

    #每次订单执行
    def excutor(self, ratio, factors, factors_df):
        #取消所有订单
        self.order_funs.cancal_existed_orders() #取消已存在的订单
        #对kline添加因子
        kline_with_features = add_features(kline_df=self.kline_df, macp_df=self.macp_df, factors_df=factors_df)
        #添加时间检测功能
        now = pd.Timestamp.now(tz='UTC')
        if now - max(kline_with_features.date) >= pd.Timedelta(days=1):
            raise ValueError("kline is outtime!")
        if len(kline_with_features.tic.unique()) != 52:
            raise ValueError("some kline is outtime!")
        #按照因子，挑选pick_ticks
        pick_ticks = self.picked_ticks(kline_with_features=kline_with_features, ratio=ratio, factors=factors)
        #获取现金和持仓数据
        cash, hold_df = self.get_hold()
        #与持仓数据融合，并添加close
        now_ticks = self.get_ticks()
        pick_ticks = pick_ticks.merge(hold_df, on="tic", how="outer")
        pick_ticks = pick_ticks.merge(now_ticks[["close", "tic"]], on="tic") #此处有问题，由于现在是kline，不是tics，close不准确
        #获取需要删除订单，并进行删除
        drop_orders = self.get_drop_orders(pick_ticks)
        if len(drop_orders) > 0:
            self.order_funs.put_divide_orders(drop_orders) #放置平仓订单一分钟
        #获取需要调整的订单，并进行调整
        cash, hold_df = self.get_hold() #再次获取现金
        new_orders = self.get_new_orders(pick_ticks, cash)  #利用现金进行交易
        if len(new_orders) > 0:
            self.order_funs.put_divide_orders(new_orders)
        print(drop_orders, new_orders)
    #ticks信息提取
    def get_ticks(self):
        result = self.marketAPI.get_tickers(instType=self.market)
        result_df = pd.DataFrame(result['data'], dtype='float64')
        result_df = result_df.rename({'instId': 'tic', 'last': 'close'}, axis=1)
        result_df['tic'] = result_df['tic'].str.replace('-', '')
        return result_df[['tic', 'close']]
    #获取交易对基础信息
    def get_tics_info(self, okex_params,market):
        publicAPI = Public.PublicAPI(**okex_params)
        tics_info = pd.DataFrame(publicAPI.get_instruments(market)["data"],dtype='float64')
        tics_info = tics_info[["instId", "minSz", 'ctVal', 'ctValCcy']]
        tics_info = tics_info.rename({'instId': 'tic', 'minSz': "minsize"}, axis=1)
        tics_info["tic"] = tics_info["tic"].str.replace("-", "")
        return tics_info
    #需要平仓的订单
    def get_drop_orders(self, pick_ticks):
        #比较简单的处理方式
        drop_ticks = pick_ticks[pick_ticks["main_factor"].isnull()]
        drop_ticks["direction"] = drop_ticks["amount"].apply(lambda x: 'short' if x < 0 else "long") #多仓卖，空仓买
        drop_ticks['amount'] = -drop_ticks['amount']
        drop_ticks = drop_ticks[drop_ticks["close"]*drop_ticks["amount"].apply(abs) >= 1]
        drop_ticks['reduceOnly'] = True
        return drop_ticks
    #新订单的计算
    def get_new_orders(self, pick_ticks, account):
        # 新增orders
        new_ticks = pick_ticks[~pick_ticks["main_factor"].isnull()]
        if len(new_ticks) == 0:
            return pd.DataFrame(columns=["tic", "direction", "amount", "close"])
        buy_cash = account / len(new_ticks[new_ticks["direction"] == "long"])
        sell_cash = - account / len(new_ticks[new_ticks["direction"] == "short"])
        new_ticks = new_ticks.fillna(0)
        new_ticks['new_amount'] = new_ticks.apply(lambda x: (buy_cash - x['value'])/x['close'] if x["direction"] == "long" else (sell_cash + x['value'])/x['close'], axis=1)
        new_ticks['reduceOnly'] = new_ticks.apply(lambda x: True if x['new_amount']*x['amount'] < 0 else False, axis=1)
        new_ticks['amount'] = new_ticks['new_amount']
        new_ticks['value'] = new_ticks['amount'] * new_ticks['close'] #计算其value值
        return new_ticks

    def reverse_long_short(self, orders):
        orders["amount"] = 0 - orders["amount"]
        orders["direction"] = orders["amount"].apply(lambda x: "buy" if x > 0 else "sell")
        return orders
    #选股函数
    def picked_ticks(self, kline_with_features, ratio, factors):
        picked_num = int(len(kline_with_features["tic"]) * ratio)
        low_list = up_list = kline_with_features
        sorted_temp = kline_with_features
        for factor in factors:
            sorted_temp = sorted_temp.sort_values(by=factor)
            low = sorted_temp.iloc[:picked_num, :]
            up = sorted_temp.iloc[-picked_num:, :]
            low_list = low_list[low_list['tic'].isin(low['tic'])]
            up_list = up_list[up_list['tic'].isin(up['tic'])]
            sorted_temp = pd.concat([low_list, up_list])
        low_list = low_list[low_list['close'] <= low_list['Trailing_factor']]
        up_list = up_list[up_list['close'] >= up_list['Trailing_factor']]
        if len(low_list) > 0 and len(up_list) > 0:
            low_list.loc[:, 'direction'] = 'short'
            up_list.loc[:, 'direction'] = 'long'
            trade_list = pd.concat([low_list, up_list])
            trade_list = trade_list[["tic", factors[0], "minsize", 'direction']]
            trade_list = trade_list.rename({factors[0]: "main_factor"}, axis=1)
            return trade_list
        else:
            return pd.DataFrame(columns=['tic', 'main_factor', 'minsize', 'direction'])
    #获取所有的数据，用于回测
    def get_all_datas(self, dir_path):
        df = pd.DataFrame()
        for file in os.listdir(dir_path):
            temp = pd.read_csv(os.path.join(dir_path, file), index_col=0)
            df = df.append(temp)
        df["date"] = pd.to_datetime(df["date"], utc=True)  # 对格式进行转换
        df = df.sort_values(by=["date"])
        df.index = pd.factorize(df["date"])[0]
        return df
    #持单量查询
    def get_hold(self):
        #获取持仓数量
        hold = self.accountAPI.get_positions(instType=self.market)
        hold_df = pd.DataFrame(hold["data"], dtype='float64')
        if len(hold_df) != 0 and self.market == 'SWAP':#不同种类的持仓需要不同的处理方式
            hold_df['instId'] = hold_df['instId'].str.replace('-', '')
            hold_df = hold_df.merge(self.tics_info, left_on='instId', right_on='tic')
            hold_df["amount"] = hold_df.apply(
                lambda x: float(x["availPos"]*x['ctVal']) if x["posSide"] == "long" else -float(x["availPos"]*x['ctVal']), axis=1)
            hold_df['value'] = hold_df.apply(lambda x: abs(x['amount']*x['last']) if x['ctValCcy'] != 'USDT' else abs(x['amount']), axis=1)
            hold_df = hold_df[["instId", "amount", 'value']]
            hold_df = hold_df.rename({'instId': 'tic'}, axis=1)
        else:
            hold_df = pd.DataFrame(columns=["tic", "value", "amount"])
        #获取现金数量
        cash = self.accountAPI.get_position_risk(instType=self.market)
        cash = pd.DataFrame(cash["data"][0]["balData"])
        cash = round(float(cash[cash["ccy"] == "USDT"]["eq"].values[0]), 2) #现金的量
        #把USDT补上以便和ticks保持一致
        return cash, hold_df
    #提取trailing_stop信息
    def extract_status(self, status_df):
        status_df = status_df.dropna()
        status_df["sign"] = status_df["sign"].apply(lambda x: json.loads(x.replace("\'", "\"")))
        status_df["percent"] = status_df["sign"].apply(lambda x: x["percent"])
        status_df["Trailing_factor"] = status_df["sign"].apply(lambda x: x["info"]["Trailing_factor"])
        status_df["down_stop"] = status_df["sign"].apply(lambda x: x["info"]["down_stop"])
        status_df[["account", "Trailing_factor","down_stop", "price"]] = status_df[["account", "Trailing_factor", "down_stop", "price"]].astype(
            "float64")
        status_df = status_df[["date", "tic", "account", "percent", "Trailing_factor", "down_stop", "price"]]
        return status_df