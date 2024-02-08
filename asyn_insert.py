from typing import Sequence

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection


async def async_insert_records_with_session(
    table: sa.Table,
    records: Sequence[dict],
    session: AsyncSession
) -> None:
    async with session.begin():
        stmt = sa.insert(table)
        await session.execute(stmt, records)


async def async_insert_records_with_connection(
    table: sa.Table,
    records: Sequence[dict],
    connection: AsyncConnection
) -> None:
    stmt = sa.insert(table)
    await connection.execute(stmt, records)