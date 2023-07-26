# 基金产品数据处理

## 项目说明

1. 本项目用于爬取天天基金网站的基金产品数据, 并对数据进行处理, 生成月级别和年级别的数据
2. 数据爬取使用selenium, 相比api获取方式, 速度较慢, 但是数据更加全面
3. 先从[基金列表网站](http://fund.eastmoney.com/fund.html#os_0;isall_0;ft_;pt_1)获取指定数目的基金信息，再去基金净值页面获取净值数据
4. 爬取净值详情部分使用多进程，减少等待时间

## 目录结构

```shell
├── format.py           # 格式化数据
├── logger.py           # 日志模块
├── logs                # 日志目录
│   └── fund.log
├── main.py             # 主程序
├── model.py            # 数据模型
├── readme.md           # 说明文档
├── requirements.txt    # 依赖包
├── service.py          # 服务模块
├── settings.py         # 配置文件
└── store               # 数据存储目录
    ├── calc_month.csv  # 月级别数据
    ├── calc_year.csv   # 年级别数据
    └── net_value.csv   # 爬取的净值数据
```

## 使用方式

1. 依照注释修改配置文件 settings.py
2. 安装依赖

```shell
pip install -r requirements.txt
```

3. 运行程序

```shell
python main.py
```

## 说明

### 数据定义

1. net_value.csv

```json
{
  "fund_code": "基金代码",
  "trading_day": "交易日",
  "unit_net_value": "单位净值",
  "cumulative_net_value": "累计净值",
  "daily_growth_rate": "日增长率",
  "purchase_status": "申购状态 (0: 开放申购, 1: 封闭期, 2: 暂停申购, 3: 限制大额申购, 4: 场内买入, -1: 其他)",
  "redeem_status": "赎回状态 (0: 开放赎回, 1: 封闭期, 2: 暂停赎回, 3: 场内卖出, -1: 其他)"
}
```

2. calc_month.csv

```json
{
  "fund_code": "基金代码",
  "fund_name": "基金名称",
  "year": "年",
  "month": "月",
  "return": "月收益"
}
```

3. calc_year.csv

```json
{
  "fund_code": "基金代码",
  "fund_name": "基金名称",
  "total_return": "总收益",
  "annual_return_ratio": "年收益",
  "annual_volatility": "年波动率",
  "sharpe": "夏普比率",
  "maximum_drawdown": "最大回撤",
  "{year}_total_return": "{year}年收益",
  "{year}_annual_return_ratio": "{year}年收益率",
  "{year}_annual_volatility": "{year}年波动率",
  "{year}_sharpe": "{year}年夏普比率",
  "{year}_maximum_drawdown": "{year}年最大回撤"
}
```
