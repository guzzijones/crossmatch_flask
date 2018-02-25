from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import create_session
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table

import pdb

# Create and engine and get the metadata
Base = declarative_base()
engine = create_engine(
              'postgresql+psycopg2://' +
              'crossmatch:hinton50@localhost:5432/crossmatch')

metadata = MetaData(bind=engine)

# Reflect each database table we need to use, using metadata


class SupMaster(Base):
    __table__ = Table('sup_master', metadata, autoload=True)


class SupplierData(Base):
    __table__ = Table('sup_data', metadata, autoload=True)


class AccountMaster(Base):
    __table__ = Table('account_master', metadata, autoload=True)


class AccountType(Base):
    __table__ = Table('account_type', metadata, autoload=True)


class ChainCode(Base):
    __table__ = Table('chain_code', metadata, autoload=True)


class CodeMatching(Base):
    __table__ = Table('code_matching', metadata, autoload=True)


class Depl(Base):
    __table__ = Table('depl', metadata, autoload=True)


class DistMaster(Base):
    __table__ = Table('dist_master', metadata, autoload=True)


class DistSupCrossreference(Base):
    __table__ = Table('dist_sup_crossreference', metadata, autoload=True)


class FoodType(Base):
    __table__ = Table('food_type', metadata, autoload=True)


class LicenseType(Base):
    __table__ = Table('license_type', metadata, autoload=True)


class Rad(Base):
    __table__ = Table('rad', metadata, autoload=True)


class RadInvc(Base):
    __table__ = Table('rad_invc', metadata, autoload=True)


class RetailerXref(Base):
    __table__ = Table('retailer_xref', metadata, autoload=True)


class SMan(Base):
    __table__ = Table('sman', metadata, autoload=True)


class States(Base):
    __table__ = Table('states', metadata, autoload=True)


class SupIncomingFiles(Base):
    __table__ = Table('sup_incoming_files', metadata, autoload=True)


class TradeChannel(Base):
    __table__ = Table('trade_channel', metadata, autoload=True)


class ZipCode(Base):
    __table__ = Table('zipcode', metadata, autoload=True)


class LiscenseType(Base):
    __table__ = Table('license_type', metadata, autoload=True)


class DB(object):
    def __init__(self):
        # Create a session to use the tables
        self.session = create_session(bind=engine)
        self.t = 1


if __name__ == "__main__":
    database = DB()
    pdb.set_trace()
    values = database.session.query(SupMaster).all()
    pass
