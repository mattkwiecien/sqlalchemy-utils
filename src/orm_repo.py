from abc import ABC
from contextlib import contextmanager
from typing import Generator, TypeVar

from sqlalchemy.orm import Session, sessionmaker

from db_item import DbItem

T = TypeVar("T", bound=DbItem)


class OrmRepo(ABC):
    def __init__(self, session_factory: sessionmaker[Session]):
        self.session_factory = session_factory

    @contextmanager
    def new_session(self, begin_txn: bool = True) -> Generator[Session, None, None]:
        session = self.session_factory()
        try:
            if begin_txn:
                with session.begin():
                    yield session
            else:
                yield session
        finally:
            session.close()

    def add(self, item: T) -> T:
        with self.new_session() as session:
            session.merge(item)
        return item

    def update(self, item: T) -> T:
        with self.new_session() as session:
            pk = list(item.__table__.primary_key)[0].name
            result = session.get(type(item), getattr(item, pk))
            if result:
                for k, v in item.as_dict().items():
                    setattr(result, k, v)
                return item.create(result.as_dict())
            else:
                raise ValueError(
                    f"No item found for {item.__tablename__}: {pk}={getattr(item, pk)}."
                )

        return item

    def get(self, item: T) -> T:
        with self.new_session() as session:
            pk = list(item.__table__.primary_key)[0].name
            result = session.get(type(item), getattr(item, pk))
            if result:
                return item.create(result.as_dict())
            else:
                raise ValueError(
                    f"No item found for {item.__tablename__}: {pk}={getattr(item, pk)}."
                )

    def delete(self, item: T) -> None:
        with self.new_session() as session:
            pk = list(item.__table__.primary_key)[0].name
            result = session.get(type(item), getattr(item, pk))
            if result:
                session.delete(result)
            else:
                raise ValueError(
                    f"No item found for {item.__tablename__}: {pk}={getattr(item, pk)}."
                )

    def delete_all(self, item: T) -> None:
        with self.new_session() as session:
            session.query(type(item)).delete()

    def list(self, item: T, where_clause: dict = {}) -> list[T]:
        with self.new_session() as session:
            result = session.query(type(item)).filter_by(**where_clause).all()
            if result:
                return [item.create(row.as_dict()) for row in result]
            else:
                raise ValueError(
                    f"No items found for {item.__tablename__}: {where_clause}"
                )

    def delete_where(self, item: T, where_clause: dict) -> None:
        with self.new_session() as session:
            result = session.query(type(item)).filter_by(**where_clause).all()
            if result:
                for row in result:
                    session.delete(row)
            else:
                raise ValueError(
                    f"No items found for {item.__tablename__}: {where_clause}"
                )
