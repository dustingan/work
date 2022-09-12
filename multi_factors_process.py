from data_process.factors_processor import atr, Trailing_factor
import pandas as pd
import numpy as np
def fill_data(temp):
    now = pd.Timestamp.now(tz='utc')
    temp.loc[:, 'date'] = pd.to_datetime(temp['date'], utc=True)
    time_index = pd.DataFrame(pd.date_range(temp['date'].min(),now,freq='D'),columns=['date'])
    df = temp.merge(time_index,on='date',how='outer')
    df = df.replace(0,np.nan)
    df = df.fillna(method='ffill')
    return df
#由于此为长期策略，无需了解ticks数据，仅采用kline即可
#输入的kline_df 应该是过滤好不要的交易对，以及时间格式转换好。
def add_features(kline_df, macp_df, factors_df):
    kline = pd.DataFrame()
    for tic in kline_df['tic'].unique():
        if tic[:-8] not in ["USDC", "BUSD", "UST", "DAI", "USDP", "USDN", "FEI"]:
            temp = kline_df[kline_df["tic"] == tic]
            temp = fill_data(temp)
            temp = temp.sort_values(by="date")
            temp.loc[:,'pct']= temp["close"].pct_change(1) #用于计算收益率
            temp.loc[:,'r1']= temp["close"].pct_change(7)  #1周动量因子
            temp.loc[:,'r2']= temp["close"].pct_change(14)#2周动量因子
            temp.loc[:,'r3']= temp["close"].pct_change(21)#3周动量因子
            temp.loc[:,'r4']= temp["close"].pct_change(28)#4周动量因子
            temp.loc[:, "prc"] = (temp["close"] - temp["close"].rolling(7).mean()) / temp["close"].rolling(
                7).std()  # 价格因子
            temp.loc[:, "maxdprc"] = (temp["high"] - temp["high"].rolling(7).mean()) / temp["high"].rolling(
                7).std()  # 最高价格因子
            temp.loc[:,"prcvol"] = (temp["volume"].rolling(7).mean()*temp["close"]) #交易量因子
            temp.loc[:,"stdprcvol"] = (temp["volume"].rolling(7).std()) #交易量波动因子
            #添加趋势截断因子
            temp_factors = factors_df[factors_df['tic'] == tic]
            if len(temp_factors) > 0:
                temp = atr(temp, temp_factors['n_atr'].iloc[-1])
                temp = Trailing_factor(df=temp, up_multi=temp_factors['up_multi'].iloc[-1], down_multi=temp_factors['down_multi'].iloc[-1])
            else:
                print(tic, ' failed add features')
                continue
            temp = temp.iloc[-1, :]
            kline = kline.append(temp)
    #处理macp，由于macp不容易变，所以删除错误0值后，选取其最后一个macp值即可
    macp_df = macp_df[macp_df['macp'] != 0.0]
    macp_df = macp_df.groupby("tic").agg({"macp":"last"})
    macp_df = macp_df.reset_index()
    #添加macp因子
    kline = kline.merge(macp_df, on='tic')
    return kline


def all_add_features(kline_df, macp_df, factors_df):
    kline = pd.DataFrame()
    for tic in kline_df['tic'].unique():
        if tic[:-8] not in ["USDC", "BUSD", "UST", "DAI", "USDP", "USDN", "FEI"]:
            temp = kline_df[kline_df["tic"] == tic]
            temp = fill_data(temp)
            temp = temp.sort_values(by="date")
            temp.loc[:,'pct']= temp["close"].pct_change(1) #用于计算收益率
            temp.loc[:,'r1']= temp["close"].pct_change(7)  #1周动量因子
            temp.loc[:,'r2']= temp["close"].pct_change(14)#2周动量因子
            temp.loc[:,'r3']= temp["close"].pct_change(21)#3周动量因子
            temp.loc[:,'r4']= temp["close"].pct_change(28)#4周动量因子
            temp.loc[:, "prc"] = (temp["close"] - temp["close"].rolling(7).mean()) / temp["close"].rolling(
                7).std()  # 价格因子
            temp.loc[:, "maxdprc"] = (temp["high"] - temp["high"].rolling(7).mean()) / temp["high"].rolling(
                7).std()  # 最高价格因子
            temp.loc[:,"prcvol"] = (temp["volume"].rolling(7).mean()*temp["close"]) #交易量因子
            temp.loc[:,"stdprcvol"] = (temp["volume"].rolling(7).std()) #交易量波动因子
            #添加趋势截断因子
            temp_factors = factors_df[factors_df['tic'] == tic]
            if len(temp_factors) > 0:
                temp = atr(temp, temp_factors['n_atr'].iloc[-1])
                temp = Trailing_factor(df=temp, up_multi=temp_factors['up_multi'].iloc[-1], down_multi=temp_factors['down_multi'].iloc[-1])
            else:
                print(tic, ' failed add features')
                continue
            kline = kline.append(temp)
    #处理macp，由于macp不容易变，所以删除错误0值后，选取其最后一个macp值即可
    macp_df = macp_df[macp_df['macp'] != 0.0]
    #添加macp因子
    kline = kline.merge(macp_df, on=['tic','date'])
    return kline