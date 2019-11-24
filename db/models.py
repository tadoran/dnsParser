# coding: utf-8
from sqlalchemy import CHAR, Column, Date, ForeignKey, Index, String, Table
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Brand(Base):
    __tablename__ = 'brands'

    brand_id = Column(INTEGER(11), primary_key=True)
    brand_name = Column(String(45), unique=True)


class BshDepartment(Base):
    __tablename__ = 'bsh_departments'

    department_id = Column(INTEGER(1), primary_key=True)
    department_name = Column(String(10), unique=True)

    def __init__(self, department_name):
        self.department_name = department_name

class City(Base):
    __tablename__ = 'cities'

    cities_id = Column(INTEGER(11), primary_key=True)
    city_name = Column(String(45), unique=True)
    city_name_DNS = Column(String(45), unique=True)
    city_archive = Column(String(255))
    city_name_file = Column(String(45), unique=True)
    city_oblast = Column(String(45))
    city_region = Column(String(12), index=True)
    city_hash = Column(String(36))


class Shop(Base):
    __tablename__ = 'shops'

    id = Column(INTEGER(11), primary_key=True)
    MD5 = Column(CHAR(32), unique=True)
    Name = Column(String(100))
    Phone = Column(String(50))
    worktime = Column(String(100))
    Address = Column(String(150))

class VibGroup(Base):
    __tablename__ = 'vib_groups'

    Id = Column(INTEGER(11), primary_key=True)
    nameGroup = Column(String(255), unique=True)
    Department = Column(ForeignKey('bsh_departments.department_id'), nullable=False, index=True)
    GrouppingStep = Column(INTEGER(11))

    bsh_department = relationship('BshDepartment')

    def __init__(self, nameGroup, Department, GrouppingStep = 1000):
        self.nameGroup = nameGroup
        self.Department = Department
        self.GrouppingStep = GrouppingStep

class Device(Base):
    __tablename__ = 'devices'

    device_article = Column(INTEGER(11), primary_key=True, unique=True)
    group_ID = Column(ForeignKey('vib_groups.Id', ondelete='CASCADE', onupdate='CASCADE'), index=True)
    brand_ID = Column(ForeignKey('brands.brand_id', ondelete='CASCADE', onupdate='CASCADE'), index=True)
    device_name_dns = Column(String(255), nullable=False)
    device_name_bsh = Column(String(255))

    brand = relationship('Brand')
    vib_group = relationship('VibGroup')


class Availability(Base):
    __tablename__ = 'availability'
    __table_args__ = (
        Index('UK_dateShopDevice', 'date', 'shop', 'device_article', unique=True),
        Index('FK_date'  , 'date'          , unique=False),
        Index('FK_shop'  , 'shop'          , unique=False)
    )

    id = Column(INTEGER(11), primary_key=True)
    device_article = Column(ForeignKey('devices.device_article', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, index=True)
    price = Column(INTEGER(6))
    prozaPass = Column(INTEGER(6))
    shop = Column(ForeignKey('shops.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)

    device = relationship('Device')
    shop1 = relationship('Shop')