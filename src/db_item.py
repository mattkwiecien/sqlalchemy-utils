from typing import Self

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class DbItem(Base):
    __abstract__ = True
    __tablename__ = "__abstract__"

    def __repr__(self):
        return f"{self.__tablename__} - {self.as_dict()}"

    def as_dict(self) -> dict:
        return {k: v for k, v in vars(self).items() if not k.startswith("_")}

    @classmethod
    def create(cls, data: dict) -> Self:
        return cls(**data)
