import logging

from typing import List, Optional, Union, Dict

from sqlalchemy import Select, and_, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from pepdbagent.const import PKG_NAME, DEFAULT_TAG_VERSION, LATEST_SCHEMA_VERSION
from pepdbagent.db_utils import BaseEngine, SchemaRecords, SchemaTags, SchemaVersions, User
from pepdbagent.exceptions import (
    SchemaAlreadyExistsError,
    SchemaVersionDoesNotExistError,
    SchemaDoesNotExistError,
    SchemaTagAlreadyExistsError,
    SchemaTagDoesNotExistError,
    SchemaVersionAlreadyExistsError,
)
from pepdbagent.models import (
    SchemaRecordAnnotation,
    SchemaVersionAnnotation,
    PaginationResult,
    SchemaVersionSearchResult,
    SchemaSearchResult,
    UpdateSchemaRecordFields,
    UpdateSchemaVersionFields,
)

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseSchema:
    """
    Class that represents SchemaRecords in Database.

    While using this class, user can create, retrieve, delete, and update schemas from database
    """

    def __init__(self, pep_db_engine: BaseEngine):
        """
        :param pep_db_engine: pepdbengine object with sa engine
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def get(self, namespace: str, name: str, version: str) -> dict:
        """
        Get schema from the database.

        :param namespace: user namespace
        :param name: schema name
        :param version: schema version

        :return: schema dict
        """

        with Session(self._sa_engine) as session:
            if version == LATEST_SCHEMA_VERSION:
                schema_obj = session.scalar(
                    select(SchemaVersions)
                    .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                    .where(
                        and_(
                            SchemaRecords.namespace == namespace,
                            SchemaRecords.name == name,
                        )
                    )
                    .order_by(SchemaVersions.version.desc())
                )

            else:

                schema_obj = session.scalar(
                    select(SchemaVersions)
                    .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                    .where(
                        and_(
                            SchemaRecords.namespace == namespace,
                            SchemaRecords.name == name,
                            SchemaVersions.version == version,
                        )
                    )
                )

            if not schema_obj:
                raise SchemaVersionDoesNotExistError(
                    f"Schema '{name}' does not exist in the database"
                )

            return schema_obj.schema_value

    def create(
        self,
        namespace: str,
        name: str,
        schema_value: dict,
        version: str = DEFAULT_TAG_VERSION,
        description: str = "",
        lifecycle_stage: str = "",
        maintainers: str = "",
        contributors: str = "",
        release_notes: str = "",
        tags: Optional[Union[List[str], str, Dict[str, str], List[Dict[str, str]]]] = None,
        private: bool = False,  # TODO: for simplicity was not implemented yet
    ) -> None:
        """
        Create or update schema in the database.

        :param namespace: user namespace
        :param name: schema name
        :param schema_value: schema dict
        :param version: schema version [Default: "1.0.0"]
        :param description: schema description [Default: ""]
        :param lifecycle_stage: schema lifecycle stage [Default: ""]
        :param maintainers: schema maintainers [Default: ""]
        :param contributors: schema contributors [Default: ""]
        :param release_notes: schema release notes [Default: ""]
        :param tags: schema tags [Default: None]
        :param private: schema privacy [Default: False]

        :return: None
        """

        tags = self._unify_tags(tags)

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name)
                )
            )

            if schema_obj:
                raise SchemaAlreadyExistsError(f"Schema '{name}' already exists in the database")

            user = session.scalar(select(User).where(User.namespace == namespace))

            if not user:
                user = User(namespace=namespace)
                session.add(user)
                session.commit()

            user.number_of_schemas += 1

            schema_obj = SchemaRecords(
                namespace=namespace,
                name=name,
                description=description,
                maintainers=maintainers,
                lifecycle_stage=lifecycle_stage,
                private=private,
            )

            session.add(schema_obj)

            schema_version_obj = SchemaVersions(
                schema_mapping=schema_obj,
                version=version,
                schema_value=schema_value,
                release_notes=release_notes,
                contributors=contributors,
            )

            for tag_name, tag_value in tags.items():
                tag_obj = session.scalar(select(SchemaTags).where(SchemaTags.tag_name == tag_name))
                if not tag_obj:
                    tag_obj = SchemaTags(
                        tag_name=tag_name, tag_value=tag_value, schema_mapping=schema_version_obj
                    )
                    session.add(tag_obj)

            session.add(schema_version_obj)
            session.commit()

        return None

    def add_version(
        self,
        namespace: str,
        name: str,
        version: str,
        schema_value: dict,
        release_notes: str = "",
        contributors: str = "",
        overwrite: bool = False,
        tags: Optional[Union[List[str], str, Dict[str, str], List[Dict[str, str]]]] = None,
    ) -> None:

        tags = self._unify_tags(tags)

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name)
                )
            )
            if not schema_obj:
                raise SchemaDoesNotExistError(
                    f"Schema '{name}' does not exist in the database. Unable to add version."
                )

            version_obj = session.scalar(
                select(SchemaVersions)
                .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                .where(
                    and_(
                        SchemaRecords.namespace == namespace,
                        SchemaRecords.name == name,
                        SchemaVersions.version == version,
                    )
                )
            )

            if version_obj:
                if not overwrite:
                    raise SchemaVersionAlreadyExistsError(
                        f"Schema '{name}' with version '{version}' already exists in the database"
                    )

                return self.update_schema_version(
                    namespace,
                    name,
                    version,
                    update_fields=UpdateSchemaVersionFields(
                        schema_value=schema_value,
                        release_notes=release_notes,
                        contributors=contributors,
                    ),
                )

            schema_obj.last_update_date = func.now()

            schema_version_obj = SchemaVersions(
                schema_id=schema_obj.id,
                version=version,
                schema_value=schema_value,
                release_notes=release_notes,
                contributors=contributors,
            )

            for tag_name, tag_value in tags.items():
                tag_obj = SchemaTags(
                    tag_name=tag_name, tag_value=tag_value, schema_mapping=schema_version_obj
                )
                session.add(tag_obj)

            session.add(schema_version_obj)
            session.commit()

        return None

    def update_schema_version(
        self,
        namespace: str,
        name: str,
        version: str,
        update_fields: Union[UpdateSchemaVersionFields, dict],
    ) -> None:
        """
        Update schema version in the database.

        :param namespace: user namespace
        :param name: schema name
        :param version: schema version
        :param update_fields: fields to be updated. Fields are optional, and include:
            - contributors: str
            - schema_value: dict
            - release_notes: str
        """
        if isinstance(update_fields, dict):
            update_fields = UpdateSchemaVersionFields(**update_fields)
        update_fields = update_fields.model_dump(exclude_unset=True, exclude_defaults=True)

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaVersions)
                .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                .where(
                    and_(
                        SchemaRecords.namespace == namespace,
                        SchemaRecords.name == name,
                        SchemaVersions.version == version,
                    )
                )
            )

            if not schema_obj:
                raise SchemaVersionDoesNotExistError(
                    f"Schema '{name}' with version '{version}' does not exist in the database. Unable to update version."
                )
            schema_obj.last_update_date = func.now()

            for field, value in update_fields.items():
                setattr(schema_obj, field, value)
                if field == "schema_value":
                    flag_modified(schema_obj, field)

            session.commit()

    def update_schema_record(
        self,
        namespace: str,
        name: str,
        update_fields: Union[UpdateSchemaRecordFields, dict],
    ) -> None:
        """
        Update schema record in the database.

        :param namespace: user namespace
        :param name: schema name
        :param update_fields: fields to be updated. Fields are optional, and include:
            - maintainers: str
            - lifecycle_stage: str
            - private: bool
            - name: str
        """

        if isinstance(update_fields, dict):
            update_fields = UpdateSchemaRecordFields(**update_fields)

        update_fields = update_fields.model_dump(exclude_unset=True, exclude_defaults=True)

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name)
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(f"Schema '{name}' does not exist in the database")

            for field, value in update_fields.items():
                setattr(schema_obj, field, value)

            session.commit()

    def schema_exist(self, namespace: str, name: str) -> bool:
        """
        Check if schema exists in the database.

        :param namespace: user namespace
        :param name: schema name

        :return: True if schema exists, False otherwise
        """

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name)
                )
            )
            return True if schema_obj else False

    def version_exist(self, namespace: str, name: str, version: str) -> bool:
        """
        Check if schema version exists in the database.

        :param namespace: user namespace
        :param name: schema name
        :param version: schema version

        :return: True if schema version exists, False otherwise
        """

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaVersions)
                .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                .where(
                    and_(
                        SchemaRecords.namespace == namespace,
                        SchemaRecords.name == name,
                        SchemaVersions.version == version,
                    )
                )
            )
            return True if schema_obj else False

    def get_schema_info(self, namespace: str, name: str) -> SchemaRecordAnnotation:
        """
        Get schema information from the database.

        :param namespace: user namespace
        :param name: schema name

        :return: SchemaRecordAnnotation
        """

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name)
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(f"Schema '{name}' does not exist in the database")

            return SchemaRecordAnnotation(
                namespace=schema_obj.namespace,
                schema_name=schema_obj.name,
                description=schema_obj.description,
                latest_released_version=schema_obj.versions_mapping[0].version,
                maintainers=schema_obj.maintainers,
                private=schema_obj.private,
                last_update_date=schema_obj.last_update_date,
                lifecycle_stage=schema_obj.lifecycle_stage,
            )

    def get_version_info(self, namespace: str, name: str, version: str) -> SchemaVersionAnnotation:
        """
        Get schema version information from the database.

        :param namespace: user namespace
        :param name: schema name
        :param version: schema version

        :return: SchemaVersionAnnotation
        """

        with Session(self._sa_engine) as session:

            # if user provided "latest" version
            if version == LATEST_SCHEMA_VERSION:
                version_obj = session.scalar(
                    select(SchemaVersions)
                    .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                    .where(and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name))
                    .order_by(SchemaVersions.version.desc())
                    .limit(1)
                )
            else:
                version_obj = session.scalar(
                    select(SchemaVersions)
                    .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                    .where(
                        and_(
                            SchemaRecords.namespace == namespace,
                            SchemaRecords.name == name,
                            SchemaVersions.version == version,
                        )
                    )
                )

            if not version_obj:
                raise SchemaVersionDoesNotExistError(
                    f"Schema '{name}' with version '{version}' does not exist in the database"
                )

            return SchemaVersionAnnotation(
                namespace=version_obj.schema_mapping.namespace,
                schema_name=version_obj.schema_mapping.name,
                version=version_obj.version,
                contributors=version_obj.contributors,
                release_notes=version_obj.release_notes,
                tags={tag.tag_name: tag.tag_value for tag in version_obj.tags_mapping},
                release_date=version_obj.release_date,
                last_update_date=version_obj.last_update_date,
            )

    def fetch_schemas(
        self,
        namespace: str = None,
        name: str = None,
        maintainer: str = None,
        lifecycle_stage: str = None,
        latest_version: str = None,
        page: int = 0,
        page_size: int = 10,
        order_by: str = "update_date",
        order_desc: bool = False,
    ) -> SchemaSearchResult:
        """
        Get schemas with providing filters.
        If not filters provided, return all schemas.

        :param namespace: user namespace [Default: None]. If None, search in all namespaces
        :param name: schema name [Default: None]
        :param maintainer: schema maintainer [Default: None]
        :param lifecycle_stage: schema lifecycle stage [Default: None]
        :param latest_version: schema latest version [Default: None]

        :param page: page number [Default: 0]
        :param page_size: number of schemas per page [Default: 0]
        :param order_by: sort the result-set by the information
            Options: ["name", "update_date"]
            [Default: update_date]
        :param order_desc: Sort the records in descending order. [Default: False]

        :return: {
            pagination: {page: int,
                        page_size: int,
                        total: int},
            results: [SchemaRecordAnnotation]
        """

        # filters = [
        #     SchemaRecords.namespace == namespace if namespace else None,
        #     SchemaRecords.name == name if name else None,
        #     SchemaRecords.maintainers == maintainer if maintainer else None,
        #     SchemaRecords.lifecycle_stage == lifecycle_stage if lifecycle_stage else None,
        # ]
        filters = [
            SchemaRecords.namespace.ilike(f"%{namespace}%") if namespace else None,
            SchemaRecords.name.ilike(f"%{name}%") if name else None,
            SchemaRecords.maintainers.ilike(f"%{maintainer}%") if maintainer else None,
            (
                SchemaRecords.lifecycle_stage.ilike(f"%{lifecycle_stage}%")
                if lifecycle_stage
                else None
            ),
        ]

        # Remove None values before applying and_
        conditions = [f for f in filters if f is not None]

        statement = (
            select(SchemaRecords).where(and_(*conditions)) if conditions else select(SchemaRecords)
        )
        statement_count = (
            select(func.count(SchemaRecords.id)).where(and_(*conditions))
            if conditions
            else select(func.count(SchemaRecords.id))
        )

        with Session(self._sa_engine) as session:
            total = session.scalar(statement_count)

            statement = self._add_order_by_schemas_keyword(statement, by=order_by, desc=order_desc)

            results_objects = session.scalars(statement.limit(page_size).offset(page * page_size))
            return SchemaSearchResult(
                pagination=PaginationResult(
                    page=page,
                    page_size=page_size,
                    total=total,
                ),
                results=[
                    SchemaRecordAnnotation(
                        namespace=result.namespace,
                        schema_name=result.name,
                        latest_released_version=result.versions_mapping[0].version,
                        description=result.description,
                        maintainers=result.maintainers,
                        private=result.private,
                        last_update_date=result.last_update_date,
                    )
                    for result in results_objects
                ],
            )

    def query_schemas(
        self,
        namespace: str = None,
        search_str: str = "",
        page: int = 0,
        page_size: int = 10,
        order_by: str = "update_date",
        order_desc: bool = False,
    ) -> SchemaSearchResult:
        """
        Search schemas in the database with pagination.

        :param namespace: user namespace [Default: None]. If None, search in all namespaces
        :param search_str: query string. [Default: ""]. If empty, return all schemas
        :param page: page number [Default: 0]
        :param page_size: number of schemas per page [Default: 0]
        :param order_by: sort the result-set by the information
            Options: ["name", "update_date"]
            [Default: update_date]
        :param order_desc: Sort the records in descending order. [Default: False]

        :return: {
            pagination: {page: int,
                        page_size: int,
                        total: int},
            results: [SchemaRecordAnnotation]
        """

        search_str = search_str.lower() if search_str else ""

        where_statement = or_(
            SchemaRecords.name.ilike(f"%{search_str}%"),
            SchemaRecords.description.ilike(f"%{search_str}%"),
        )
        if namespace:
            where_statement = and_(where_statement, SchemaRecords.namespace == namespace)

        with Session(self._sa_engine) as session:
            total = session.scalar(select(func.count(SchemaRecords.id)).where(where_statement))
            statement = (
                select(SchemaRecords)
                .where(where_statement)
                .limit(page_size)
                .offset(page * page_size)
            )
            statement = self._add_order_by_schemas_keyword(statement, by=order_by, desc=order_desc)
            results_objects = session.scalars(statement)

            return SchemaSearchResult(
                pagination=PaginationResult(
                    page=page,
                    page_size=page_size,
                    total=total,
                ),
                results=[
                    SchemaRecordAnnotation(
                        namespace=result.namespace,
                        schema_name=result.name,
                        latest_released_version=result.versions_mapping[0].version,
                        description=result.description,
                        maintainers=result.maintainers,
                        private=result.private,
                        last_update_date=result.last_update_date,
                    )
                    for result in results_objects
                ],
            )

    def query_schema_version(
        self,
        namespace: str,
        name: str,
        tag: str = None,
        search_str: str = "",
        page: int = 0,
        page_size: int = 10,
    ) -> SchemaVersionSearchResult:
        """
        Search schema versions in the database with pagination.

        :param namespace: user namespace
        :param name: schema name
        :param tag: tag name. [Default: None]. If None, return versions with all tags
        :param search_str: query string. [Default: ""]. If empty, return all schemas
        :param page: result page number [Default: 10]
        :param page_size: number of schemas per page [Default: 10]

        :return: {
            pagination: {page: int,
                        page_size: int,
                        total: int},
            results: [SchemaVersionAnnotation]
        """

        search_str = search_str.lower() if search_str else ""

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name)
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(f"Schema '{name}' does not exist in the database")

            where_statement = and_(
                SchemaRecords.namespace == namespace,
                SchemaRecords.name == name,
                or_(
                    SchemaVersions.version.ilike(f"%{search_str}%"),
                    SchemaVersions.release_notes.ilike(f"%{search_str}%"),
                ),
            )

            if tag:
                where_statement = and_(where_statement, SchemaTags.tag_name == tag)
                total_statement = (
                    select(func.count(SchemaVersions.id))
                    .join(SchemaRecords)
                    .join(SchemaTags)
                    .where(where_statement)
                )
                find_statement = (
                    select(SchemaVersions)
                    .join(SchemaRecords)
                    .join(SchemaTags)
                    .where(where_statement)
                )

            else:
                total_statement = (
                    select(func.count(SchemaVersions.id))
                    .join(SchemaRecords)
                    .where(where_statement)
                )
                find_statement = select(SchemaVersions).join(SchemaRecords).where(where_statement)

            total = session.scalar(total_statement)

            results_objects = session.scalars(
                find_statement.order_by(SchemaVersions.version.desc())
                .limit(page_size)
                .offset(page * page_size)
            ).unique()

            return SchemaVersionSearchResult(
                pagination=PaginationResult(
                    page=page,
                    page_size=page_size,
                    total=total,
                ),
                results=[
                    SchemaVersionAnnotation(
                        namespace=result.schema_mapping.namespace,
                        schema_name=result.schema_mapping.name,
                        version=result.version,
                        contributors=result.contributors,
                        release_notes=result.release_notes,
                        tags={tag.tag_name: tag.tag_value for tag in result.tags_mapping},
                        release_date=result.release_date,
                        last_update_date=result.last_update_date,
                    )
                    for result in results_objects
                ],
            )

    def delete_schema(self, namespace: str, name: str) -> None:
        """
        Delete schema from the database.

        :param namespace: user namespace
        :param name: schema name
        :return: None
        """

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(SchemaRecords.namespace == namespace, SchemaRecords.name == name)
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(f"Schema '{name}' does not exist in the database")

            statement = select(User).where(User.namespace == namespace)
            user = session.scalar(statement)
            if user:
                user.number_of_schemas -= 1
                session.commit()

            session.delete(schema_obj)
            session.commit()

    def delete_version(self, namespace: str, name: str, version: str) -> None:
        """
        Delete version of the schema

        :param namespace: Namespace of the schema
        :param name: Name of the schema
        :param version: Version of the Schema

        :raise: SchemaVersionDoesNotExistError if version doesn't exist
        :return: None
        """
        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaVersions)
                .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                .where(
                    and_(
                        SchemaRecords.namespace == namespace,
                        SchemaRecords.name == name,
                        SchemaVersions.version == version,
                    )
                )
            )
            if not schema_obj:
                raise SchemaVersionDoesNotExistError(
                    f"Schema '{name}' with version '{version}' does not exist in the database. Unable to update version."
                )

            session.delete(schema_obj)
            session.commit()

    def add_tag_to_schema(
        self,
        namespace: str,
        name: str,
        version: str,
        tag: Optional[Union[List[str], str, Dict[str, str]]],
    ) -> None:
        """
        Add tag to the schema

        :param namespace: Namespace of the schema
        :param name: Name of the schema
        :param version: Version of the Schema
        :param tag: Tag to be added. Can be a string, list of strings or dictionaries

        :raise: SchemaVersionDoesNotExistError if version doesn't exist
        :return: None
        """

        tag = self._unify_tags(tag)

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaVersions)
                .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                .where(
                    and_(
                        SchemaRecords.namespace == namespace,
                        SchemaRecords.name == name,
                        SchemaVersions.version == version,
                    )
                )
            )
            if not schema_obj:
                raise SchemaVersionDoesNotExistError(
                    f"Schema '{name}' with version '{version}' does not exist in the database. Unable to add tag."
                )
            if isinstance(tag, str):
                tag = [tag]

            for tag_name, tag_value in tag.items():
                tag_obj = session.scalar(select(SchemaTags).where(SchemaTags.tag_name == tag_name))
                if not tag_obj:
                    tag_obj = SchemaTags(
                        tag_name=tag_name, tag_value=tag_value, schema_mapping=schema_obj
                    )
                    session.add(tag_obj)
                else:
                    raise SchemaTagAlreadyExistsError(
                        f"Tag '{tag_name}' already exists in the schema"
                    )

            session.commit()

    def remove_tag_from_schema(self, namespace: str, name: str, version: str, tag: str) -> None:
        """
        Remove tag from the schema

        :param namespace: Namespace of the schema
        :param name: Name of the schema
        :param version: Version of the Schema
        :param tag: Tag to be removed

        :raise: SchemaVersionDoesNotExistError if version doesn't exist
        :return: None
        """
        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaVersions)
                .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                .where(
                    and_(
                        SchemaRecords.namespace == namespace,
                        SchemaRecords.name == name,
                        SchemaVersions.version == version,
                    )
                )
            )
            if not schema_obj:
                raise SchemaVersionDoesNotExistError(
                    f"Schema '{name}' with version '{version}' does not exist in the database. Unable to remove tag."
                )

            tag_obj = session.scalar(
                select(SchemaTags).where(
                    SchemaTags.tag_name == tag, SchemaTags.schema_version_id == schema_obj.id
                )
            )
            if not tag_obj:
                raise SchemaTagDoesNotExistError(f"Tag '{tag}' does not exist in the schema")

            session.delete(tag_obj)
            session.commit()

    @staticmethod
    def _add_order_by_schemas_keyword(
        statement: Select, by: str = "update_date", desc: bool = False
    ) -> Select:
        """
        Add order by clause to sqlalchemy statement

        :param statement: sqlalchemy representation of a SELECT statement.
        :param by: sort the result-set by the information
            Options: ["name", "update_date"]
            [Default: "update_date"]
        :param desc: Sort the records in descending order. [Default: False]
        :return: sqlalchemy representation of a SELECT statement with order by keyword
        """
        if by == "update_date":
            order_by_obj = SchemaRecords.last_update_date
        elif by == "name":
            order_by_obj = SchemaRecords.name
        else:
            _LOGGER.warning(
                f"order by: '{by}' statement is unavailable. Projects are sorted by 'update_date'"
            )
            order_by_obj = SchemaRecords.last_update_date

        if desc and by == "name":
            order_by_obj = order_by_obj.desc()

        elif by != "name" and not desc:
            order_by_obj = order_by_obj.desc()

        return statement.order_by(order_by_obj)

    def _unify_tags(
        self, tags: Optional[Union[List[str], str, Dict[str, str], List[Dict[str, str]]]]
    ) -> [Dict[str, str]]:
        """
        Convert provided tags to one standard

        :param tags: tags to be converted from types: str, dict, list of str, list of dict

        :raise: ValueError if tags are not in the correct format
        :return: dictionary of tags
        """
        if not tags:
            tags = {}
        if tags == (None,):
            tags = {}
        elif isinstance(tags, str):
            tags = {tags: None}
        elif isinstance(tags, dict):
            pass
        elif isinstance(tags, list):
            if all(isinstance(tag, str) for tag in tags):
                tags = {tag: None for tag in tags}
            else:
                raise ValueError(
                    f"tags should be a list of strings or a list of dictionaries. Tag values: {tags}"
                )
        else:
            raise ValueError(
                f"tags should be a list of strings or a list of dictionaries. Tag values: {tags}"
            )
        return tags
