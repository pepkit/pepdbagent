from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session
from sqlalchemy import PrimaryKeyConstraint, FetchedValue
from sqlalchemy import Table
from sqlalchemy import select

from sqlalchemy import String, BigInteger
from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy import event
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.engine import URL

from typing import Optional, Any
import datetime
import logging

from pepdbagent.const import POSTGRES_DIALECT
from pepdbagent.exceptions import SchemaError

_LOGGER = logging.getLogger("pepdbagent")


class BIGSERIAL(BigInteger):
    pass


@compiles(BIGSERIAL, "postgresql")
def compile_bigserial_pg(type_, compiler, **kw):
    return "BIGSERIAL"


class Base(MappedAsDataclass, DeclarativeBase):
    type_annotation_map = {
        dict[str, Any]: JSONB,
    }


@event.listens_for(Base.metadata, "after_create")
def receive_after_create(target, connection, tables, **kw):
    """
    listen for the 'after_create' event
    """
    if tables:
        _LOGGER.warning("A table was created")
        print("A table was created")
    else:
        _LOGGER.info("A table was not created")
        print("A table was not created")


class Projects(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(BIGSERIAL, server_default=FetchedValue())
    namespace: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(primary_key=True)
    tag: Mapped[str] = mapped_column(primary_key=True)
    digest: Mapped[str] = mapped_column(String(32))
    project_value: Mapped[dict[str, Any]]
    private: Mapped[bool]
    number_of_samples: Mapped[int]
    submission_date: Mapped[datetime.datetime]
    last_update_date: Mapped[datetime.datetime]
    # schema: Mapped[Optional[str]]

    __table_args__ = (PrimaryKeyConstraint("namespace", "name", "tag", name="id"),)


class BaseEngine:
    """
    A class with base methods, that are used in several classes. e.g. fetch_one or fetch_all
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        database: str = "pep-db",
        user: str = None,
        password: str = None,
        drivername: str = POSTGRES_DIALECT,
        dsn: str = None,
        echo: bool = False,
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.
        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param dsn: libpq connection string using the dsn parameter
        (e.g. 'postgresql://user_name:password@host_name:port/db_name')
        """
        if not dsn:
            dsn = URL.create(
                host=host,
                port=port,
                database=database,
                username=user,
                password=password,
                drivername=drivername,
            )

        self._engine = create_engine(dsn, echo=echo)

        session = Session(self._engine)
        try:
            session.execute(select(Projects)).first()
        except ProgrammingError:
            raise SchemaError()

    def create_schema(self):
        Base.metadata.create_all(self._engine)

    @property
    def session(self):
        return self._start_session()

    @property
    def engine(self):
        return self._engine

    def _start_session(self):
        session = Session(self.engine)
        try:
            session.execute(select(Projects)).first()
        except ProgrammingError:
            raise SchemaError()

        return session

    @staticmethod
    def _create_dsn_string(
        host: str = "localhost",
        port: int = 5432,
        database: str = "pep-db",
        user: str = None,
        password: str = None,
        dialect: str = POSTGRES_DIALECT,
    ) -> str:
        """
        Using host, port, database, user, and password and dialect

        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param dialect: DB dialect, specific implementation or variant of a database system. [Default: postgresql]
        :return: sqlalchemy connection string
        """
        return f"{dialect}://{user}:{password}@{host}:{port}/{database}"


def main():
    # engine = BaseEngine(dsn='postgresql://postgres:docker@localhost:5432/pep-db')
    engine = BaseEngine(
        host="localhost",
        port=5432,
        database="pep-db",
        user="postgres",
        password="docker",
        echo=True,
    )
    # engine.create_schema()
    ff = engine.session


if __name__ == "__main__":
    main()
