from typing import List, Tuple, Any

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromiumService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from multiprocessing import Process, Manager, Pool
import traceback
import pandas as pd
import numpy as np
from decimal import Decimal

import settings
from logger import logger
from format import net_value_formatter, net_value_to_dict, get_calc_year_cols_sequence
from model import Database, bulk_add, NetValue


pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("max_colwidth", None)
pd.set_option("display.width", 1000)


class Crawler(object):
    """
    爬虫类
    """

    def __init__(self):
        self.chrome = None
        self.db = None
        self.config()

    def config(self):
        """
        初始化配置
        :return:
        """

        # 初始化浏览器驱动器
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        self.chrome = webdriver.Chrome(
            options=options,
            service=ChromiumService(
                ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            ),
        )

        if settings.STORE_TYPE == "MYSQL":
            # 初始化数据库连接
            self.db = Database(settings.MYSQL_URL)
            self.db.connect()
            self.db.create_tables()
        elif settings.STORE_TYPE != "CSV":
            raise Exception("STORE_TYPE must be MYSQL or CSV")

    def get_fund_codes(self) -> list[tuple[Any, Any]]:
        """
        获取产品代码列表
        :return: 产品列表
        """
        self.chrome.get("http://fund.eastmoney.com/fund.html#os_0;isall_0;ft_;pt_1")
        fund_tr_list = self.chrome.find_elements(
            By.CSS_SELECTOR, "#oTable > tbody > tr"
        )
        fund_code_list = []
        for fund_tr in fund_tr_list:
            fund_code = fund_tr.find_element(By.CSS_SELECTOR, "td.bzdm").text
            fund_name = fund_tr.find_element(
                By.CSS_SELECTOR, "td.tol > nobr > a:nth-child(1)"
            ).text
            fund_code_list.append((fund_code, fund_name))
        return fund_code_list[: settings.MAX_FUND_NUM]

    def get_net_value(self, fund_code: str) -> list[NetValue]:
        """
        获取净值数据
        :param fund_code: 基金代码
        :return: 净值字典列表
        """
        self.chrome.get("http://fundf10.eastmoney.com/jjjz_" + fund_code + ".html")

        net_value_obj_list = []
        is_last_page = False

        while not is_last_page:
            try:
                WebDriverWait(self.chrome, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#jztable > table > tbody > tr")
                    )
                )
            except Exception as e:
                logger.warning("获取净值数据失败，基金代码：" + fund_code)
                logger.error(e)
                logger.error(traceback.format_exc())
                return []
            net_value_tr_list = self.chrome.find_elements(
                By.CSS_SELECTOR, "#jztable > table > tbody > tr"
            )

            for net_value_tr in net_value_tr_list:
                net_value_obj = {"fund_code": fund_code}
                trading_day_str = net_value_tr.find_element(
                    By.CSS_SELECTOR, "td:nth-child(1)"
                ).text
                net_value_obj["trading_day"] = datetime.strptime(
                    trading_day_str, "%Y-%m-%d"
                ).strftime("%Y%m%d")
                net_value_obj["unit_net_value"] = net_value_tr.find_element(
                    By.CSS_SELECTOR, "td:nth-child(2)"
                ).text
                net_value_obj["cumulative_net_value"] = net_value_tr.find_element(
                    By.CSS_SELECTOR, "td:nth-child(3)"
                ).text
                if (
                    net_value_obj["unit_net_value"] == ""
                    or net_value_obj["cumulative_net_value"] == ""
                ):
                    continue
                daily_growth_rate = net_value_tr.find_element(
                    By.CSS_SELECTOR, "td:nth-child(4)"
                ).text
                net_value_obj["daily_growth_rate"] = (
                    daily_growth_rate if daily_growth_rate != "--" else "0"
                )
                purchase_status = net_value_tr.find_element(
                    By.CSS_SELECTOR, "td:nth-child(5)"
                ).text
                purchase_status = purchase_status.strip()

                net_value_obj["purchase_status"] = purchase_status

                redeem_status = net_value_tr.find_element(
                    By.CSS_SELECTOR, "td:nth-child(6)"
                ).text
                redeem_status = redeem_status.strip()

                net_value_obj["redeem_status"] = redeem_status
                net_value_obj_list.append(net_value_formatter(net_value_obj))

            page_btns = self.chrome.find_elements(
                By.CSS_SELECTOR, "#pagebar > div.pagebtns > label"
            )
            next_page_btn = page_btns[-1]
            class_attribute = next_page_btn.get_attribute("class")
            is_last_page = "end" in class_attribute.split()
            if not is_last_page:
                next_page_btn.click()
        return net_value_obj_list

    def save_net_value(self, net_value_obj_list) -> Any:
        """
        存储净值数据
        :param net_value_obj_list: 净值字典列表
        :return: Any
        """
        if settings.STORE_TYPE == "MYSQL":
            bulk_add(self.db.Session(), net_value_obj_list)
        elif settings.STORE_TYPE == "CSV":
            net_value_csv_list = [
                net_value_to_dict(net_val_obj) for net_val_obj in net_value_obj_list
            ]
            df = pd.DataFrame(net_value_csv_list)
            df.drop("_sa_instance_state", axis=1, inplace=True)
            return df
        else:
            raise Exception("不支持的存储类型")


def crawl(fund_code: str, fund_name: str, df_dict: dict):
    """
    单个基金产品净值爬虫
    :param df_dict: 多进程中共享的字典
    :param fund_name: 基金名称
    :param fund_code: 基金代码
    :return:
    """
    crawler = Crawler()
    try:
        logger.info("开始爬取基金产品净值，基金代码：" + fund_code)
        net_value_list = crawler.get_net_value(fund_code)
        net_value_df = crawler.save_net_value(net_value_list)
        logger.info(
            "爬取基金产品净值成功，基金代码：" + fund_code + "，净值数量：" + str(len(net_value_list)) + "条"
        )
        year_df, month_df = CalcService.calc(
            pd.DataFrame(
                [net_value_to_dict(net_val_obj) for net_val_obj in net_value_list]
            ),
            fund_code,
            fund_name,
        )

        df_dict[fund_code] = {
            "year_df": year_df,
            "month_df": month_df,
            "net_value_df": net_value_df,
        }

    except Exception as e:
        logger.error("爬取基金产品净值失败，基金代码：" + fund_code + "，错误信息：" + str(e))
        logger.error(traceback.format_exc())
    finally:
        if settings.STORE_TYPE == "MYSQL":
            crawler.db.disconnect()
        crawler.chrome.quit()


class CrawlService(object):
    def __init__(self):
        pass

    @staticmethod
    def run():
        logger.info("开始爬取基金产品净值")
        fund_crawler = Crawler()

        fund_codes = fund_crawler.get_fund_codes()
        logger.info("获取基金代码列表成功，基金数量：" + str(len(fund_codes)) + "个")
        fund_crawler.chrome.quit()

        with Manager() as manager:
            df_dict = manager.dict()

            pool = Pool(processes=settings.MAX_CONCURRENCY)

            # 执行工作进程函数
            for fund_code, fund_name in fund_codes:
                pool.apply_async(
                    crawl,
                    args=(
                        fund_code,
                        fund_name,
                        df_dict,
                    ),
                )
            pool.close()
            pool.join()

            net_value_df = pd.DataFrame()
            year_df = pd.DataFrame()
            month_df = pd.DataFrame()
            for fund_code, _df_dict in df_dict.items():
                year_df = pd.concat([year_df, _df_dict["year_df"]], ignore_index=True)
                month_df = pd.concat(
                    [month_df, _df_dict["month_df"]], ignore_index=True
                )
                if _df_dict["net_value_df"] is not None:
                    net_value_df = pd.concat(
                        [net_value_df, _df_dict["net_value_df"]], ignore_index=True
                    )

            year_df = year_df.reindex(
                columns=get_calc_year_cols_sequence(list(year_df.columns))
            )
            if settings.STORE_TYPE == "CSV":
                net_value_df.to_csv(
                    settings.CSV_READ_PATH,
                    mode="w",
                    header=True,
                    index=False,
                    encoding="utf-8-sig",
                )
            year_df.to_csv(
                settings.CSV_WRITE_DIR + "/calc_year.csv",
                mode="w",
                header=True,
                index=False,
                encoding="utf-8-sig",
            )
            month_df.to_csv(
                settings.CSV_WRITE_DIR + "/calc_month.csv",
                mode="w",
                header=True,
                index=False,
                encoding="utf-8-sig",
            )
            logger.info("爬取基金产品净值完成")


class CalcService(object):
    def __init__(self):
        pass

    @staticmethod
    def calc(df: pd.DataFrame, fund_code: str, fund_name: str):
        """
        计算指标
        :param fund_name: 基金名称
        :param fund_code: 基金代码
        :param df: 净值数据
        :return:
        """
        df = df.drop("_sa_instance_state", axis="columns")
        df["cumulative_net_value"] = df["cumulative_net_value"].apply(Decimal)
        df["trading_day"] = pd.to_datetime(df["trading_day"], format="%Y%m%d")
        df = df.sort_values(by="trading_day", ascending=True)
        df = df.reset_index(drop=True)
        total_data_dict = CalcService.calc_data(df, False)
        del total_data_dict["monthly_return"]
        total_data_dict["fund_code"] = fund_code
        total_data_dict["fund_name"] = fund_name
        dfs_by_year = {"total": total_data_dict}
        df["year"] = pd.DatetimeIndex(df["trading_day"]).year

        # 获取年份列表
        year_cols = df["year"].unique().tolist()

        # 获取年份数据列
        year_data_cols = [
            "total_return",
            "annual_return_ratio",
            "annual_volatility",
            "sharpe",
            "maximum_drawdown",
        ]

        # 年类型数据列
        year_all_cols = (
            ["fund_code", "fund_name"]
            + [year_col for year_col in year_data_cols]
            + [str(year) + "_" + col for year in year_cols for col in year_data_cols]
        )
        year_df = pd.DataFrame(columns=year_all_cols)

        month_cols = ["fund_code", "fund_name", "year", "month", "return"]
        month_df = pd.DataFrame(columns=month_cols)

        for year, group_df in df.groupby("year"):
            group_df.drop("year", axis=1, inplace=True)
            dfs_by_year[year] = CalcService.calc_data(group_df)

            for col in year_data_cols:
                total_data_dict[str(year) + "_" + col] = dfs_by_year[year][col]
            for month, returns in dfs_by_year[year]["monthly_return"].items():
                month_dict = {
                    "fund_code": fund_code,
                    "fund_name": fund_name,
                    "year": year,
                    "month": month,
                    "return": returns,
                }
                month_df = pd.concat(
                    [month_df, pd.DataFrame([month_dict])], ignore_index=True
                )

        year_df = pd.concat(
            [year_df, pd.DataFrame([total_data_dict])], ignore_index=True
        )

        return year_df, month_df

    @staticmethod
    def calc_data(df: pd.DataFrame, is_year=True):
        """
        计算传入净值数据的时间段内的指标
        (计算各种年化指标以及总指标可以使用同一函数，只需要传入不同的时间段的数据即可)
        :param df: 净值数据
        :param is_year: 是否是计算年化指标
        :return:
        """
        df.sort_values(by="trading_day", ascending=True, inplace=True)
        # 总收益
        total_return = CalcService.calc_total_return(df)
        # 年化收益率
        annual_return_ratio = CalcService.calc_annual_return_ratio(df)
        # 年化波动率
        annual_volatility = CalcService.calc_annual_volatility(df)
        # 夏普比率
        sharpe = CalcService.calc_sharpe(annual_return_ratio, annual_volatility)
        # 最大回撤
        maximum_drawdown = CalcService.calc_maximum_drawdown(df)
        # 月收益
        monthly_return = CalcService.calc_monthly_return(df)

        return {
            "total_return": total_return,
            "annual_return_ratio": annual_return_ratio,
            "annual_volatility": annual_volatility,
            "sharpe": sharpe,
            "maximum_drawdown": maximum_drawdown,
            "monthly_return": monthly_return if is_year else None,
        }

    @staticmethod
    def calc_total_return(df: pd.DataFrame) -> Decimal:
        """
        计算总收益
        :param df: 净值数据
        :return:
        """
        total_return = (
            df["cumulative_net_value"].iloc[-1] - df["cumulative_net_value"].iloc[0]
        )
        return total_return

    @staticmethod
    def calc_annual_return_ratio(df: pd.DataFrame) -> Decimal:
        """
        计算年化收益率
        :param df: 净值数据
        :return:
        """
        annual_return = (
            df["cumulative_net_value"].iloc[-1] / df["cumulative_net_value"].iloc[0]
        ) ** Decimal(
            365 / (df["trading_day"].iloc[-1] - df["trading_day"].iloc[0]).days
        ) - 1
        return annual_return.quantize(Decimal("0.0000"))

    @staticmethod
    def calc_annual_volatility(df: pd.DataFrame) -> Decimal:
        """
        计算年化波动率
        :param df: 净值数据
        :return:
        """

        # 计算每日收益率
        df["daily_returns"] = df["cumulative_net_value"].pct_change()

        # 计算日对数收益率
        df["daily_returns"] = df["daily_returns"].fillna(0)
        df["daily_returns"] = df["daily_returns"].astype(float)
        df["log_returns"] = np.log(1 + df["daily_returns"])

        # 计算日波动率（对数收益率的标准差）
        daily_volatility = df["log_returns"].std()

        # 计算年化波动率
        annual_volatility = daily_volatility * np.sqrt(len(df))  # 252个交易日
        annual_volatility = Decimal(annual_volatility)
        return annual_volatility.quantize(Decimal("0.0000"))

    @staticmethod
    def calc_sharpe(annual_return: Decimal, annual_volatility: Decimal) -> Decimal:
        """
        计算夏普比率
        :param annual_volatility:
        :param annual_return:
        :return:
        """
        if annual_volatility == 0:
            return Decimal(0)
        annual_return = (
            annual_return - Decimal(settings.RISK_FREE_RATE)
        ) / annual_volatility
        return annual_return.quantize(Decimal("0.0000"))

    @staticmethod
    def calc_maximum_drawdown(df: pd.DataFrame) -> Decimal:
        """
        计算最大回撤幅度
        :param df: 净值数据
        :return:
        """

        maximum_drawdown = (
            df["cumulative_net_value"].max() - df["cumulative_net_value"].min()
        ) / df["cumulative_net_value"].max()
        return maximum_drawdown.quantize(Decimal("0.0000"))

    @staticmethod
    def calc_annual_return(df: pd.DataFrame) -> Decimal:
        """
        计算年化收益
        :param df: 净值数据
        :return:
        """
        annual_return = (
            df["cumulative_net_value"].iloc[-1] / df["cumulative_net_value"].iloc[0]
        ) ** Decimal(
            365 / (df["trading_day"].iloc[-1] - df["trading_day"].iloc[0]).days
        ) - 1
        return annual_return.quantize(Decimal("0.0000"))

    @staticmethod
    def calc_monthly_return(df: pd.DataFrame):
        """
        计算月收益
        :param df: 净值数据
        :return:
        """
        df["month"] = pd.DatetimeIndex(df["trading_day"]).month
        month_dict = {}
        for month, group_df in df.groupby("month"):
            group_df.drop("month", axis=1, inplace=True)
            group_df.sort_values(by="trading_day", ascending=True, inplace=True)
            month_dict[month] = (
                group_df["cumulative_net_value"].iloc[-1]
                - group_df["cumulative_net_value"].iloc[0]
            )
            month_dict[month] = month_dict[month].quantize(Decimal("0.0000"))
        return month_dict
