# 存储类型 [CSV, MYSQL]
STORE_TYPE = "CSV"

# 数据库地址 (CSV类型忽略)
MYSQL_URL = "mysql+pymysql://root:123456@127.0.0.1:3306/tmp"

# CSV 数据存储地址（MYSQL类型忽略）
CSV_READ_PATH = "./store/net_value.csv"

# CSV 结果输出目录
CSV_WRITE_DIR = "./store"

# 基金爬取数量
MAX_FUND_NUM = 20

# 日志配置
LOG_CONFIG = {
    "sink": "./logs/fund.log",
    "enqueue": True,
    "rotation": "4 weeks",
    "retention": "4 months",
    "encoding": "utf-8",
    "backtrace": True,
    "diagnose": True,
    "compression": "zip",
}

# 最大并发数
MAX_CONCURRENCY = 5

# 十年期国债收益率
RISK_FREE_RATE = 2.68
