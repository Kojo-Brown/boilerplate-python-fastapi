import uuid
from typing import Any, Generic, TypeVar

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import Base

ModelT = TypeVar("ModelT", bound=Base)


class BaseRepository(Generic[ModelT]):
    """Generic async repository providing standard CRUD operations.

    Subclasses must call ``super().__init__(session, ModelClass)`` to register
    the concrete SQLAlchemy model this repository manages.
    """

    def __init__(self, session: AsyncSession, model: type[ModelT]) -> None:
        self.session = session
        self._model = model

    async def get(self, id: uuid.UUID) -> ModelT | None:
        """Fetch a single row by primary key."""
        return await self.session.get(self._model, id)

    async def get_by(self, **kwargs: Any) -> ModelT | None:
        """Fetch a single row matching ALL keyword-argument field=value filters."""
        conditions = [getattr(self._model, k) == v for k, v in kwargs.items()]
        result = await self.session.execute(
            select(self._model).where(*conditions)
        )
        return result.scalar_one_or_none()

    async def list(
        self,
        limit: int = 20,
        offset: int = 0,
    ) -> list[ModelT]:
        """Return a page of rows ordered by insertion (no guaranteed order beyond DB default)."""
        result = await self.session.execute(
            select(self._model).limit(limit).offset(offset)
        )
        return list(result.scalars().all())

    async def count(self) -> int:
        """Return the total row count for the table."""
        result = await self.session.execute(
            select(func.count()).select_from(self._model)
        )
        val: int = result.scalar_one()
        return val

    async def create(self, **kwargs: Any) -> ModelT:
        """Insert a new row, flush the session, and return the refreshed instance."""
        instance = self._model(**kwargs)
        self.session.add(instance)
        await self.session.flush()
        await self.session.refresh(instance)
        return instance

    async def update(self, id: uuid.UUID, **kwargs: Any) -> ModelT | None:
        """Update an existing row by PK and return the refreshed instance, or None."""
        instance = await self.get(id)
        if instance is None:
            return None
        for key, value in kwargs.items():
            setattr(instance, key, value)
        await self.session.flush()
        await self.session.refresh(instance)
        return instance

    async def delete(self, id: uuid.UUID) -> bool:
        """Delete a row by PK. Returns True if deleted, False if not found."""
        instance = await self.get(id)
        if instance is None:
            return False
        await self.session.delete(instance)
        await self.session.flush()
        return True
