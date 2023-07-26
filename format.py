from model import NetValue
from decimal import Decimal
from logger import logger


def net_value_formatter(obj):
    daily_growth_rate = obj["daily_growth_rate"]
    daily_growth_rate = (
        None
        if daily_growth_rate is None
        else Decimal(obj["daily_growth_rate"].strip("%")) / 100
    )
    obj["daily_growth_rate"] = daily_growth_rate
    obj["unit_net_value"] = (
        None if obj["unit_net_value"] == "" else obj["unit_net_value"]
    )
    obj["cumulative_net_value"] = (
        None if obj["cumulative_net_value"] == "" else obj["cumulative_net_value"]
    )

    purchase_dict = {
        "开放申购": 0,
        "封闭期": 1,
        "暂停申购": 2,
        "限制大额申购": 3,
        "场内买入": 4,
    }
    if obj["purchase_status"] in purchase_dict:
        obj["purchase_status"] = purchase_dict.get(obj["purchase_status"])
    else:
        logger.warning(
            "purchase_status is not in purchase_list: " + obj["purchase_status"]
        )
        obj["purchase_status"] = -1

    redeem_dict = {
        "开放赎回": 0,
        "封闭期": 1,
        "暂停赎回": 2,
        "场内卖出": 3,
    }
    if obj["redeem_status"] in redeem_dict:
        obj["redeem_status"] = redeem_dict.get(obj["redeem_status"])
    else:
        logger.warning("redeem_status is not in redeem_list: " + obj["redeem_status"])
        obj["redeem_status"] = -1

    net_value = NetValue(**obj)
    return net_value


def net_value_to_dict(net_value: NetValue):
    return net_value.__dict__


def get_calc_year_cols_sequence(cols: list):
    """
    获取计算年计算值的固定列的顺序
    :param cols:
    :return:
    """
    total_cols = [
        "fund_code",
        "fund_name",
        "total_return",
        "annual_return_ratio",
        "annual_volatility",
        "sharpe",
        "maximum_drawdown",
    ]
    year_cols = sorted(list(set(cols) - set(total_cols)))
    return total_cols + year_cols
