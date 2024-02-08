import asyncio

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from asyn_insert import async_insert_records_with_connection, async_insert_records_with_session
from async_select import async_select_records_chunks_with_session, async_select_records_with_session


class Base(AsyncAttrs, DeclarativeBase):
    pass

class Cat(Base):
    __tablename__ = "cats"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(30))
    age: Mapped[int] = mapped_column(sa.Integer())


async def main(table):
    engine =  create_async_engine('sqlite+aiosqlite:///test.db')

    records = [
        {'name': 'Nami', 'age': 5},
        {'name': 'Leia', 'age': 2}
    ]
    
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await async_insert_records_with_connection(table, records, connection)
        print('inserted with engine connection')

    Session = async_sessionmaker(engine)
    async with Session() as session:
        await async_insert_records_with_session(table, records, session)
        print('inserted with session')
        results = async_select_records_chunks_with_session(table, session)
        async for row in results:
            print(row)
    await engine.dispose()


if __name__ == '__main__':
    engine = sa.create_engine('sqlite:///test.db')
    metadata = sa.MetaData()
    Base.metadata.create_all(bind=engine)
    table = sa.Table('cats', metadata, autoload_with=engine)
    asyncio.run(main(table))