from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, BigInteger
from sqlalchemy.ext.compiler import compiles
import datetime
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, Any
from sqlalchemy import create_engine
from sqlalchemy import event

from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass
from sqlalchemy import PrimaryKeyConstraint, FetchedValue

class BIGSERIAL(BigInteger):
    pass


@compiles(BIGSERIAL, "postgresql")
def compile_bigserial_pg(type_, compiler, **kw):
    return "BIGSERIAL"


class Base(MappedAsDataclass, DeclarativeBase):
    type_annotation_map = {
        dict[str, Any]: JSONB,
    }


@event.listens_for(Base.metadata, 'after_create')
def receive_after_create(target, connection, tables, **kw):
    "listen for the 'after_create' event"
    if tables:
        print('A table was created')
    else:
        print('A table was not created')


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

    __table_args__ = (
        PrimaryKeyConstraint("namespace", "name", "tag", name="id"),
    )


def main():
    engine = create_engine('postgresql://postgres:docker@localhost:5432/pep-db', echo=True, future=True)
    asd = Base.metadata.create_all(engine)




if __name__ == "__main__":
    main()

