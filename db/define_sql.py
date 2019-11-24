from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
if __name__ == "__main__":
    from models import VibGroup, BshDepartment, Base, metadata
else:
    from db.models import VibGroup, BshDepartment, Base, metadata


def get_session():
    # engine = create_engine('sqlite:///:memory:', echo=True)
    # engine = create_engine('sqlite:///database.db', echo=True)
    engine = create_engine('sqlite:///database.db', echo=False)
    
    metadata.create_all(engine)
    # create a configured "Session" class
    Session = sessionmaker(bind=engine)
    # create a Session
    return Session()

def create_categoties():
    session = get_session()
    categories = ({
        "MDA": [
            "Варочные панели газовые", "Варочные панели электрические", "Встраиваемые посудомоечные машины",
            "Духовые шкафы электрические",
            "Посудомоечные машины", "Стиральные машины", "Холодильники", "Вытяжки"
              ],
        "CP": [
            "Блендеры погружные",
            "Блендеры стационарные", "Грили и раклетницы",
            "Измельчители", "Кофеварки капельные", "Кофемашины автоматические", "Кофемашины капсульные", "Кофемолки",
            "Кухонные комбайны",
            "Микроволновые печи", "Миксеры", "Мультиварки", "Мясорубки", "Соковыжималки", "Чайники", "Электрочайники",
            "Гладильные доски",
            "Гладильные системы", "Мешки-пылесборники", "Парогенераторы", "Пылесосы", "Утюги"
               ]
    })
    for dep_name, dep_cats in categories.items():
        department = BshDepartment(department_name = dep_name)
        session.add(department)
        session.commit()

        session.add_all(
                        [VibGroup(category, department.department_id) for category in dep_cats]
                        )
        session.commit()
    session.close()

def get_categories():
    session = get_session()
    return session.query(VibGroup).all()

if __name__ == "__main__":
    try:
        create_categoties()
    except exc.IntegrityError as e:
        print(__name__, e)
    print([category.nameGroup for category in get_categories()])
    #for category in get_categories():
    #    print(category.Id, category.nameGroup)