import datetime
import enum
import logging
from typing import List, Optional

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Enum,
    FetchedValue,
    ForeignKey,
    Result,
    Select,
    String,
    UniqueConstraint,
    event,
    select,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

from pepdbagent.const import PKG_NAME, POSTGRES_DIALECT
from pepdbagent.exceptions import SchemaError

_LOGGER = logging.getLogger(PKG_NAME)


class BIGSERIAL(BigInteger):
    pass


@compiles(BIGSERIAL, POSTGRES_DIALECT)
def compile_bigserial_pg(type_, compiler, **kw):
    return "BIGSERIAL"


@compiles(JSON, POSTGRES_DIALECT)
def compile_jsonb_pg(type_, compiler, **kw):
    return "JSON"


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


# def deliver_description(context):
#     return context.get_current_parameters()["config"]["description"]


def deliver_update_date(context):
    return datetime.datetime.now(datetime.timezone.utc)


class Projects(Base):
    """
    Projects table representation in the database
    """

    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(primary_key=True)
    namespace: Mapped[str] = mapped_column(ForeignKey("users.namespace", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column()
    tag: Mapped[str] = mapped_column()
    digest: Mapped[str] = mapped_column(String(32))
    description: Mapped[Optional[str]]
    config: Mapped[dict] = mapped_column(JSON, server_default=FetchedValue())
    private: Mapped[bool]
    number_of_samples: Mapped[int]
    number_of_stars: Mapped[int] = mapped_column(default=0)
    submission_date: Mapped[datetime.datetime]
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date,  # onupdate=deliver_update_date, # This field should not be updated, while we are adding project to favorites
    )

    schema_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("schema_versions.id", ondelete="SET NULL"), nullable=True
    )
    schema_mapping: Mapped["SchemaVersions"] = relationship("SchemaVersions", lazy="joined")

    pop: Mapped[Optional[bool]] = mapped_column(default=False)
    samples_mapping: Mapped[List["Samples"]] = relationship(
        back_populates="project_mapping", cascade="all, delete-orphan"
    )
    subsamples_mapping: Mapped[List["Subsamples"]] = relationship(
        back_populates="subsample_mapping", cascade="all, delete-orphan"
    )
    stars_mapping: Mapped[List["Stars"]] = relationship(
        back_populates="project_mapping", cascade="all, delete-orphan"
    )
    views_mapping: Mapped[List["Views"]] = relationship(
        back_populates="project_mapping", cascade="all, delete-orphan"
    )

    # Self-referential relationship. The parent project is the one that was forked to create this one.
    forked_from_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("projects.id", ondelete="SET NULL"), nullable=True
    )
    forked_from_mapping = relationship(
        "Projects",
        back_populates="forked_to_mapping",
        remote_side=[id],
        single_parent=True,
        cascade="save-update, merge, refresh-expire",
    )

    forked_to_mapping = relationship(
        "Projects",
        back_populates="forked_from_mapping",
        cascade="save-update, merge, refresh-expire",
    )

    namespace_mapping: Mapped["User"] = relationship("User", back_populates="projects_mapping")

    history_mapping: Mapped[List["HistoryProjects"]] = relationship(
        back_populates="project_mapping", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("namespace", "name", "tag"),)


class Samples(Base):
    """
    Samples table representation in the database
    """

    __tablename__ = "samples"

    id: Mapped[int] = mapped_column(primary_key=True)
    sample: Mapped[dict] = mapped_column(JSON, server_default=FetchedValue())
    project_id = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project_mapping: Mapped["Projects"] = relationship(back_populates="samples_mapping")
    sample_name: Mapped[Optional[str]] = mapped_column()
    guid: Mapped[Optional[str]] = mapped_column(nullable=False, unique=True)

    submission_date: Mapped[datetime.datetime] = mapped_column(default=deliver_update_date)
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date,
        onupdate=deliver_update_date,
    )

    parent_guid: Mapped[Optional[str]] = mapped_column(
        ForeignKey("samples.guid", ondelete="CASCADE"),
        nullable=True,
        doc="Parent sample id. Used to create a hierarchy of samples.",
    )

    parent_mapping: Mapped["Samples"] = relationship(
        "Samples", remote_side=guid, back_populates="child_mapping"
    )
    child_mapping: Mapped["Samples"] = relationship("Samples", back_populates="parent_mapping")

    views: Mapped[Optional[List["ViewSampleAssociation"]]] = relationship(
        back_populates="sample", cascade="all, delete-orphan"
    )


class Subsamples(Base):
    """
    Subsamples table representation in the database
    """

    __tablename__ = "subsamples"

    id: Mapped[int] = mapped_column(primary_key=True)
    subsample: Mapped[dict] = mapped_column(JSON, server_default=FetchedValue())
    subsample_number: Mapped[int]
    row_number: Mapped[int]
    project_id = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    subsample_mapping: Mapped["Projects"] = relationship(back_populates="subsamples_mapping")


class User(Base):
    """
    User table representation in the database
    """

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    namespace: Mapped[str] = mapped_column(nullable=False, unique=True)
    stars_mapping: Mapped[List["Stars"]] = relationship(
        back_populates="user_mapping",
        cascade="all, delete-orphan",
        order_by="Stars.star_date.desc()",
    )
    number_of_projects: Mapped[int] = mapped_column(default=0)
    number_of_schemas: Mapped[int] = mapped_column(default=0)

    projects_mapping: Mapped[List["Projects"]] = relationship(
        "Projects", back_populates="namespace_mapping"
    )
    schemas_mapping: Mapped[List["SchemaRecords"]] = relationship(
        "SchemaRecords", back_populates="user_mapping"
    )


class Stars(Base):
    """
    FavoriteProjects table representation in the database
    """

    __tablename__ = "stars"

    user_id = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    project_id = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True)
    user_mapping: Mapped[List["User"]] = relationship(back_populates="stars_mapping")
    project_mapping: Mapped["Projects"] = relationship(back_populates="stars_mapping")
    star_date: Mapped[datetime.datetime] = mapped_column(
        onupdate=deliver_update_date, default=deliver_update_date
    )


class Views(Base):
    """
    Views table representation in the database
    """

    __tablename__ = "views"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    description: Mapped[Optional[str]]

    project_id = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project_mapping = relationship("Projects", back_populates="views_mapping")

    samples: Mapped[List["ViewSampleAssociation"]] = relationship(
        back_populates="view", cascade="all, delete-orphan"
    )

    _table_args__ = (UniqueConstraint("name", "project_id"),)


class ViewSampleAssociation(Base):
    """
    Association table between views and samples
    """

    __tablename__ = "views_samples"

    sample_id = mapped_column(ForeignKey("samples.id", ondelete="CASCADE"), primary_key=True)
    view_id = mapped_column(ForeignKey("views.id", ondelete="CASCADE"), primary_key=True)
    sample: Mapped["Samples"] = relationship(back_populates="views")
    view: Mapped["Views"] = relationship(back_populates="samples")


class HistoryProjects(Base):

    __tablename__ = "project_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    user: Mapped[str] = mapped_column(ForeignKey("users.namespace", ondelete="SET NULL"))
    update_time: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=deliver_update_date
    )
    project_yaml: Mapped[dict] = mapped_column(JSON, server_default=FetchedValue())

    project_mapping: Mapped["Projects"] = relationship(
        "Projects", back_populates="history_mapping"
    )
    sample_changes_mapping: Mapped[List["HistorySamples"]] = relationship(
        back_populates="history_project_mapping", cascade="all, delete-orphan"
    )


class UpdateTypes(enum.Enum):
    """
    Enum for the type of update
    """

    UPDATE = "update"
    INSERT = "insert"
    DELETE = "delete"


class HistorySamples(Base):

    __tablename__ = "sample_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    history_id: Mapped[int] = mapped_column(ForeignKey("project_history.id", ondelete="CASCADE"))
    guid: Mapped[str] = mapped_column(nullable=False)
    parent_guid: Mapped[Optional[str]] = mapped_column(nullable=True)
    sample_json: Mapped[dict] = mapped_column(JSON, server_default=FetchedValue())
    change_type: Mapped[UpdateTypes] = mapped_column(Enum(UpdateTypes), nullable=False)

    history_project_mapping: Mapped["HistoryProjects"] = relationship(
        "HistoryProjects", back_populates="sample_changes_mapping"
    )


class SchemaRecords(Base):
    __tablename__ = "schema_records"

    id: Mapped[int] = mapped_column(primary_key=True)
    namespace: Mapped[str] = mapped_column(ForeignKey("users.namespace", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(nullable=False)
    maintainers: Mapped[str] = mapped_column(nullable=True)
    lifecycle_stage: Mapped[str] = mapped_column(nullable=True)
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    private: Mapped[bool] = mapped_column(default=False)
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date, onupdate=deliver_update_date
    )

    __table_args__ = (UniqueConstraint("namespace", "name"),)

    versions_mapping: Mapped[List["SchemaVersions"]] = relationship(
        "SchemaVersions",
        back_populates="schema_mapping",
        cascade="all, delete-orphan",
        order_by="SchemaVersions.version.desc()",
    )
    user_mapping: Mapped["User"] = relationship("User", back_populates="schemas_mapping")


class SchemaVersions(Base):
    __tablename__ = "schema_versions"

    id: Mapped[int] = mapped_column(primary_key=True)
    schema_id: Mapped[int] = mapped_column(ForeignKey("schema_records.id", ondelete="CASCADE"))
    version: Mapped[str] = mapped_column(nullable=False)
    schema_value: Mapped[dict] = mapped_column(JSON, server_default=FetchedValue())
    release_date: Mapped[datetime.datetime] = mapped_column(default=deliver_update_date)
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date, onupdate=deliver_update_date
    )
    contributors: Mapped[Optional[str]] = mapped_column(nullable=True)
    release_notes: Mapped[Optional[str]] = mapped_column(nullable=True)

    __table_args__ = (UniqueConstraint("schema_id", "version"),)

    schema_mapping: Mapped["SchemaRecords"] = relationship(
        "SchemaRecords", back_populates="versions_mapping"
    )

    tags_mapping: Mapped[List["SchemaTags"]] = relationship(
        "SchemaTags", back_populates="schema_mapping", lazy="joined", cascade="all, delete-orphan"
    )


class SchemaTags(Base):
    __tablename__ = "schema_tags"

    id: Mapped[int] = mapped_column(primary_key=True)
    tag_name: Mapped[str] = mapped_column(nullable=False)
    tag_value: Mapped[str] = mapped_column(nullable=True)
    schema_version_id: Mapped[int] = mapped_column(
        ForeignKey("schema_versions.id", ondelete="CASCADE")
    )

    schema_mapping: Mapped["SchemaVersions"] = relationship(
        "SchemaVersions", back_populates="tags_mapping"
    )


class TarNamespace(Base):

    __tablename__ = "namespace_archives"

    id: Mapped[int] = mapped_column(primary_key=True)
    namespace: Mapped[str] = mapped_column(ForeignKey("users.namespace", ondelete="CASCADE"))
    file_path: Mapped[str] = mapped_column(nullable=False)
    creation_date: Mapped[datetime.datetime] = mapped_column(default=deliver_update_date)
    number_of_projects: Mapped[int] = mapped_column(default=0)
    file_size: Mapped[int] = mapped_column(nullable=False)


class BedBaseStats(Base):
    __tablename__ = "bedbase_stats"

    id: Mapped[int] = mapped_column(primary_key=True)
    gse: Mapped[str] = mapped_column()
    gsm: Mapped[str] = mapped_column()
    sample_name: Mapped[str] = mapped_column(nullable=True)
    genome: Mapped[Optional[str]] = mapped_column(nullable=True, default="")
    last_update_date: Mapped[Optional[str]] = mapped_column()
    submission_date: Mapped[Optional[str]] = mapped_column()


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

    def delete_schema(self, engine=None) -> None:
        """
        Delete sql schema in the database.

        :param engine: sqlalchemy engine [Default: None]
        :return: None
        """
        if not engine:
            engine = self._engine
        Base.metadata.drop_all(engine)
        return None
