from typing import AsyncGenerator

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection


async def async_select_records_with_session(
    table: sa.Table,
    session: AsyncSession
) -> AsyncGenerator[dict, None]:
    async with session.begin():
        stmt = sa.select(table)
        results = await session.execute(stmt)
        for row in results.fetchall():
            yield dict(row._mapping)


async def async_insert_records_with_connection(
    table: sa.Table,
    connection: AsyncConnection
) -> AsyncGenerator[dict, None]:
    stmt = sa.insert(table)
    results = await connection.execute(stmt)
    for row in results.fetchall():
        yield dict(row._mapping)


async def async_select_records_chunks_with_session(
    table: sa.Table,
    session: AsyncSession,
    chunk_size = 2
) -> AsyncGenerator[dict, None]:
    async with session.begin():
        stmt = sa.select(table)
        results = await session.stream(stmt)
        async for row in results.yield_per(chunk_size):
            yield dict(row._mapping)