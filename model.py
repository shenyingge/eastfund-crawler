from sqlalchemy import (
    Column,
    DECIMAL,
    DateTime,
    Integer,
    text,
    create_engine,
)
from sqlalchemy.dialects.mysql import VARCHAR

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

Base = declarative_base()


class NetValue(Base):
    __tablename__ = "net_value"
    id = Column(Integer, primary_key=True, comment="自增主键")
    trading_day = Column(VARCHAR(9), nullable=False, comment="交易日")
    fund_code = Column(VARCHAR(50), nullable=False, comment="基金代码")
    unit_net_value = Column(DECIMAL(12, 4), comment="单位净值")
    cumulative_net_value = Column(DECIMAL(12, 4), comment="累计净值")
    daily_growth_rate = Column(DECIMAL(6, 4), default=0, comment="日增长率")
    purchase_status = Column(Integer, nullable=False, comment="申购状态")
    redeem_status = Column(Integer, nullable=False, comment="赎回状态")
    create_at = Column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    update_at = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    )


class Database:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = None
        self.Session = None

    def connect(self):
        self.engine = create_engine(self.db_uri)
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def disconnect(self):
        if self.engine:
            self.engine.dispose()


def transactional(func):
    def wrapper(session, *args, **kwargs):
        try:
            result = func(session, *args, **kwargs)
            session.commit()
            return result
        except Exception as e:
            session.rollback()
            logging.exception("An error occurred during transaction: {}".format(str(e)))
        finally:
            session.close()

    return wrapper


@transactional
def bulk_add(session, obj_list, bulk_size=1000):
    for i in range(0, len(obj_list), bulk_size):
        session.bulk_save_objects(obj_list[i : i + bulk_size])
