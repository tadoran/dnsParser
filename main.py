from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from  db.models import metadata, Availability, Brand, BshDepartment, City, Device, Shop, VibGroup
# engine = create_engine('mysql+pymysql://tadoran:epA7Jgpa@localhost/dnsavailability')


categories = [
        "Варочные панели газовые", "Варочные панели электрические", "Встраиваемые посудомоечные машины",
        "Духовые шкафы электрические",
        "Посудомоечные машины", "Стиральные машины", "Холодильники", "Вытяжки", "Блендеры погружные",
        "Блендеры стационарные", "Грили и раклетницы",
        "Измельчители", "Кофеварки капельные", "Кофемашины автоматические", "Кофемашины капсульные", "Кофемолки",
        "Кухонные комбайны",
        "Микроволновые печи", "Миксеры", "Мультиварки", "Мясорубки", "Соковыжималки", "Чайники", "Электрочайники",
        "Гладильные доски",
        "Гладильные системы", "Мешки-пылесборники", "Парогенераторы", "Пылесосы", "Утюги"
    ]


#engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///database.db', echo=True)

metadata.create_all(engine)

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()

department1 = BshDepartment("ALL")
session.add(department1)
try:
    session.commit()
except exc.IntegrityError:
    pass

for cat in categories:
    group = VibGroup(cat, department1.department_id)
    session.add(group)
try:
    session.commit()
except exc.IntegrityError:
    pass


session.close()
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
# db = SQLAlchemy(engine)
#
# import db.models