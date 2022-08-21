import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json
import os

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=60), unique=True)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)
    publisher = relationship(Publisher, backref="publisher")


class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    book = relationship(Book, backref="book")
    shop = relationship(Shop, backref="shop")


class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.NUMERIC, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    stock = relationship(Stock, backref="stock")


def create_tables(engine):
    table = input("1 - Создать таблицы\n"
                  "2 - Удалить и заново создать таблицы:\n")
    if table == 1:
        Base.metadata.create_all(engine)
        print("Таблицы созданы")
    else:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        print("Таблицы созданы заново")

DSN = 'postgresql://postgres:*********@localhost:5432/SQLAlchemy'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()
file_name = "tests_data.json"
base_path = os.getcwd()
full_path = os.path.join(base_path, file_name)

with open(full_path, 'r') as fd:
    data = json.load(fd)
for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))

session.commit()


info = input("Выберите критерий выборки: имя или id издателя:\n")
if info == "имя":
    info1 = input("Введите имя издателя:\n")
    subq = session.query(Publisher).filter(Publisher.name == info1).subquery()
elif info == "id":
    info1 = input("Введите id издателя:\n")
    subq = session.query(Publisher).filter(Publisher.id == info1).subquery()

q = session.query(Shop).join(Stock.shop).join(Stock.book).join(subq, Book.id_publisher == subq.c.id)
print("магазины, продающие издателя:")
for s in q.all():
    print(s.name)

session.close()