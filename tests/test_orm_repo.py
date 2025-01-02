import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import sessionmaker

from db_item import DbItem
from orm_repo import OrmRepo


class MockDbItem(DbItem):
    __tablename__ = "some_table"
    id_col = Column(Integer(), nullable=False, primary_key=True)
    display_name = Column(String(500))


@pytest.fixture(name="orm_repo")
def fixture_orm_repo():
    engine = create_engine("sqlite://")
    session_factory = sessionmaker(bind=engine)

    repo = OrmRepo(session_factory)
    with repo.new_session() as session:
        session.execute(
            text("CREATE TABLE some_table (id_col INTEGER, display_name TEXT);")
        )

    return repo


def test_add_item_succeeds(orm_repo: OrmRepo) -> None:
    # Arrange
    mock_item = MockDbItem(id_col=1, display_name="test")
    # Act
    result = orm_repo.add(mock_item)
    read = orm_repo.get(mock_item)
    # Assert
    assert result is not None
    assert result.display_name == mock_item.display_name
    assert result.id_col == mock_item.id_col
    assert read is not None
    assert read.display_name == mock_item.display_name
    assert read.id_col == mock_item.id_col
