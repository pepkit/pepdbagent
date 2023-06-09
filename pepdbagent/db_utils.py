import datetime
import logging
from typing import Any, Optional

from sqlalchemy import (
    BigInteger,
    FetchedValue,
    PrimaryKeyConstraint,
    Result,
    Select,
    String,
    event,
    select,
    TIMESTAMP,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
)

from pepdbagent.const import POSTGRES_DIALECT, PKG_NAME
from pepdbagent.exceptions import SchemaError

_LOGGER = logging.getLogger(PKG_NAME)


class BIGSERIAL(BigInteger):
    pass


@compiles(BIGSERIAL, POSTGRES_DIALECT)
def compile_bigserial_pg(type_, compiler, **kw):
    return "BIGSERIAL"


@compiles(JSONB, POSTGRES_DIALECT)
def compile_jsonb_pg(type_, compiler, **kw):
    return "JSONB"


class Base(DeclarativeBase):
    type_annotation_map = {datetime.datetime: TIMESTAMP(timezone=True)}


@event.listens_for(Base.metadata, "after_create")
def receive_after_create(target, connection, tables, **kw):
    """
    listen for the 'after_create' event
    """
    if tables:
        _LOGGER.info("A table was created")
    else:
        _LOGGER.info("A table was not created")


class Projects(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(BIGSERIAL, server_default=FetchedValue())
    namespace: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(primary_key=True)
    tag: Mapped[str] = mapped_column(primary_key=True)
    digest: Mapped[str] = mapped_column(String(32))
    project_value: Mapped[dict] = mapped_column(JSONB, server_default=FetchedValue())
    private: Mapped[bool]
    number_of_samples: Mapped[int]
    submission_date: Mapped[datetime.datetime]
    last_update_date: Mapped[datetime.datetime]
    pep_schema: Mapped[Optional[str]]

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
        :param drivername: driver used in
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
        self.create_schema(self._engine)
        self.check_db_connection()

    def create_schema(self, engine=None):
        """
        Create sql schema in the database.

        :param engine: sqlalchemy engine [Default: None]
        :return: None
        """
        if not engine:
            engine = self._engine
        Base.metadata.create_all(engine)
        return None

    def session_execute(self, statement: Select) -> Result:
        """
        Execute statement using sqlalchemy statement

        :param statement: SQL query or a SQL expression that is constructed using
            SQLAlchemy's SQL expression language
        :return: query result represented with declarative base
        """
        _LOGGER.debug(f"Executing statement: {statement}")
        with Session(self._engine) as session:
            query_result = session.execute(statement)

        return query_result

    @property
    def session(self):
        """
        :return: started sqlalchemy session
        """
        return self._start_session()

    @property
    def engine(self):
        return self._engine

    def _start_session(self):
        session = Session(self.engine)
        try:
            session.execute(select(Projects).limit(1))
        except ProgrammingError:
            raise SchemaError()

        return session

    def check_db_connection(self):
        try:
            self.session_execute(select(Projects).limit(1))
        except ProgrammingError:
            raise SchemaError()
