import logging

from sqlalchemy import Select, and_, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from pepdbagent.const import DEFAULT_TAG_VERSION, LATEST_SCHEMA_VERSION, PKG_NAME
from pepdbagent.db_utils import (
    BaseEngine,
    SchemaRecords,
    SchemaTags,
    SchemaVersions,
    User,
)
from pepdbagent.exceptions import (
    SchemaAlreadyExistsError,
    SchemaDoesNotExistError,
    SchemaTagAlreadyExistsError,
    SchemaTagDoesNotExistError,
    SchemaVersionAlreadyExistsError,
    SchemaVersionDoesNotExistError,
)
from pepdbagent.models import (
    PaginationResult,
    SchemaRecordAnnotation,
    SchemaSearchResult,
    SchemaVersionAnnotation,
    SchemaVersionSearchResult,
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
        Args:
            pep_db_engine: PEPDatabaseAgent engine object.
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def get(self, namespace: str, name: str, version: str) -> dict:
        """Get a schema value from the database.

        Args:
            namespace: User namespace.
            name: Schema name.
            version: Schema version (use "latest" for the most recent).

        Returns:
            Schema value dict.

        Raises:
            SchemaVersionDoesNotExistError: If the schema or version does not exist.
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
        tags: list[str] | str | dict[str, str] | list[dict[str, str]] | None = None,
        private: bool = False,  # TODO: for simplicity was not implemented yet
    ) -> None:
        """Create a new schema record in the database.

        Args:
            namespace: User namespace.
            name: Schema name.
            schema_value: Schema content as a dict.
            version: Initial version string.
            description: Schema description.
            lifecycle_stage: Lifecycle stage, e.g., "stable" or "deprecated".
            maintainers: Comma-separated maintainer names.
            contributors: Comma-separated contributor names.
            release_notes: Release notes for this version.
            tags: Tags to associate with the schema version.
            private: Mark schema as private if True.

        Raises:
            SchemaAlreadyExistsError: If a schema with the same name exists in the namespace.
        """

        tags = self._unify_tags(tags)

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(
                        SchemaRecords.namespace == namespace, SchemaRecords.name == name
                    )
                )
            )

            if schema_obj:
                raise SchemaAlreadyExistsError(
                    f"Schema '{name}' already exists in the database"
                )

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
                tag_obj = session.scalar(
                    select(SchemaTags).where(SchemaTags.tag_name == tag_name)
                )
                if not tag_obj:
                    tag_obj = SchemaTags(
                        tag_name=tag_name,
                        tag_value=tag_value,
                        schema_mapping=schema_version_obj,
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
        tags: list[str] | str | dict[str, str] | list[dict[str, str]] | None = None,
    ) -> None:

        tags = self._unify_tags(tags)

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(
                        SchemaRecords.namespace == namespace, SchemaRecords.name == name
                    )
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
                    tag_name=tag_name,
                    tag_value=tag_value,
                    schema_mapping=schema_version_obj,
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
        update_fields: UpdateSchemaVersionFields | dict,
    ) -> None:
        """Update fields of an existing schema version.

        Args:
            namespace: User namespace.
            name: Schema name.
            version: Schema version to update.
            update_fields: Fields to update — contributors, schema_value, and/or release_notes.

        Raises:
            SchemaVersionDoesNotExistError: If the version does not exist.
        """
        if isinstance(update_fields, dict):
            update_fields = UpdateSchemaVersionFields(**update_fields)
        update_fields = update_fields.model_dump(
            exclude_unset=True, exclude_defaults=True
        )

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
        update_fields: UpdateSchemaRecordFields | dict,
    ) -> None:
        """Update metadata fields of a schema record.

        Args:
            namespace: User namespace.
            name: Schema name.
            update_fields: Fields to update — maintainers, lifecycle_stage, private, and/or name.

        Raises:
            SchemaDoesNotExistError: If the schema does not exist.
        """

        if isinstance(update_fields, dict):
            update_fields = UpdateSchemaRecordFields(**update_fields)

        update_fields = update_fields.model_dump(
            exclude_unset=True, exclude_defaults=True
        )

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(
                        SchemaRecords.namespace == namespace, SchemaRecords.name == name
                    )
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(
                    f"Schema '{name}' does not exist in the database"
                )

            for field, value in update_fields.items():
                setattr(schema_obj, field, value)

            session.commit()

    def schema_exist(self, namespace: str, name: str) -> bool:
        """Check whether a schema exists in the database.

        Args:
            namespace: User namespace.
            name: Schema name.

        Returns:
            True if the schema exists.
        """

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(
                        SchemaRecords.namespace == namespace, SchemaRecords.name == name
                    )
                )
            )
            return True if schema_obj else False

    def version_exist(self, namespace: str, name: str, version: str) -> bool:
        """Check whether a specific schema version exists.

        Args:
            namespace: User namespace.
            name: Schema name.
            version: Schema version.

        Returns:
            True if the version exists.
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
        """Get metadata for a schema record.

        Args:
            namespace: User namespace.
            name: Schema name.

        Returns:
            SchemaRecordAnnotation with schema metadata.

        Raises:
            SchemaDoesNotExistError: If the schema does not exist.
        """

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(
                        SchemaRecords.namespace == namespace, SchemaRecords.name == name
                    )
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(
                    f"Schema '{name}' does not exist in the database"
                )

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

    def get_version_info(
        self, namespace: str, name: str, version: str
    ) -> SchemaVersionAnnotation:
        """Get metadata for a specific schema version.

        Args:
            namespace: User namespace.
            name: Schema name.
            version: Schema version (use "latest" for the most recent).

        Returns:
            SchemaVersionAnnotation with version metadata and tags.

        Raises:
            SchemaVersionDoesNotExistError: If the version does not exist.
        """

        with Session(self._sa_engine) as session:
            # if user provided "latest" version
            if version == LATEST_SCHEMA_VERSION:
                version_obj = session.scalar(
                    select(SchemaVersions)
                    .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                    .where(
                        and_(
                            SchemaRecords.namespace == namespace,
                            SchemaRecords.name == name,
                        )
                    )
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
        """Get schemas matching the given filters.

        Returns all schemas if no filters are provided.

        Args:
            namespace: Restrict to this namespace (default: all namespaces).
            name: Partial name match.
            maintainer: Partial maintainer match.
            lifecycle_stage: Partial lifecycle stage match.
            latest_version: Partial latest version match.
            page: Page number (zero-based).
            page_size: Number of results per page.
            order_by: Sort field — "name" or "update_date".
            order_desc: Sort in descending order if True.

        Returns:
            SchemaSearchResult with pagination and list of SchemaRecordAnnotation objects.
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
            select(SchemaRecords).where(and_(*conditions))
            if conditions
            else select(SchemaRecords)
        )
        statement_count = (
            select(func.count(SchemaRecords.id)).where(and_(*conditions))
            if conditions
            else select(func.count(SchemaRecords.id))
        )

        with Session(self._sa_engine) as session:
            total = session.scalar(statement_count)

            statement = self._add_order_by_schemas_keyword(
                statement, by=order_by, desc=order_desc
            )

            results_objects = session.scalars(
                statement.limit(page_size).offset(page * page_size)
            )
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
        """Search schemas by name and description with pagination.

        Args:
            namespace: Restrict to this namespace (default: all namespaces).
            search_str: Text to search in name and description (default: all schemas).
            page: Page number (zero-based).
            page_size: Number of results per page.
            order_by: Sort field — "name" or "update_date".
            order_desc: Sort in descending order if True.

        Returns:
            SchemaSearchResult with pagination and list of SchemaRecordAnnotation objects.
        """

        search_str = search_str.lower() if search_str else ""

        where_statement = or_(
            SchemaRecords.name.ilike(f"%{search_str}%"),
            SchemaRecords.description.ilike(f"%{search_str}%"),
        )
        if namespace:
            where_statement = and_(
                where_statement, SchemaRecords.namespace == namespace
            )

        with Session(self._sa_engine) as session:
            total = session.scalar(
                select(func.count(SchemaRecords.id)).where(where_statement)
            )
            statement = (
                select(SchemaRecords)
                .where(where_statement)
                .limit(page_size)
                .offset(page * page_size)
            )
            statement = self._add_order_by_schemas_keyword(
                statement, by=order_by, desc=order_desc
            )
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
        """Search versions of a schema with pagination.

        Args:
            namespace: User namespace.
            name: Schema name.
            tag: Filter by tag name (default: all tags).
            search_str: Text to search in version and release_notes.
            page: Page number (zero-based).
            page_size: Number of results per page.

        Returns:
            SchemaVersionSearchResult with pagination and list of SchemaVersionAnnotation objects.

        Raises:
            SchemaDoesNotExistError: If the schema does not exist.
        """

        search_str = search_str.lower() if search_str else ""

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(
                        SchemaRecords.namespace == namespace, SchemaRecords.name == name
                    )
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(
                    f"Schema '{name}' does not exist in the database"
                )

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
                find_statement = (
                    select(SchemaVersions).join(SchemaRecords).where(where_statement)
                )

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
                        tags={
                            tag.tag_name: tag.tag_value for tag in result.tags_mapping
                        },
                        release_date=result.release_date,
                        last_update_date=result.last_update_date,
                    )
                    for result in results_objects
                ],
            )

    def delete_schema(self, namespace: str, name: str) -> None:
        """Delete a schema record and all its versions from the database.

        Args:
            namespace: User namespace.
            name: Schema name.

        Raises:
            SchemaDoesNotExistError: If the schema does not exist.
        """

        with Session(self._sa_engine) as session:
            schema_obj = session.scalar(
                select(SchemaRecords).where(
                    and_(
                        SchemaRecords.namespace == namespace, SchemaRecords.name == name
                    )
                )
            )

            if not schema_obj:
                raise SchemaDoesNotExistError(
                    f"Schema '{name}' does not exist in the database"
                )

            statement = select(User).where(User.namespace == namespace)
            user = session.scalar(statement)
            if user:
                user.number_of_schemas -= 1
                session.commit()

            session.delete(schema_obj)
            session.commit()

    def delete_version(self, namespace: str, name: str, version: str) -> None:
        """Delete a specific schema version.

        Args:
            namespace: Namespace of the schema.
            name: Name of the schema.
            version: Version to delete.

        Raises:
            SchemaVersionDoesNotExistError: If the version does not exist.
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
        tag: list[str] | str | dict[str, str] | None,
    ) -> None:
        """Add a tag to a schema version.

        Args:
            namespace: Namespace of the schema.
            name: Name of the schema.
            version: Schema version.
            tag: Tag to add — string, list of strings, or dict of name→value pairs.

        Raises:
            SchemaVersionDoesNotExistError: If the version does not exist.
            SchemaTagAlreadyExistsError: If the tag already exists on the version.
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
                tag_obj = session.scalar(
                    select(SchemaTags).where(SchemaTags.tag_name == tag_name)
                )
                if not tag_obj:
                    tag_obj = SchemaTags(
                        tag_name=tag_name,
                        tag_value=tag_value,
                        schema_mapping=schema_obj,
                    )
                    session.add(tag_obj)
                else:
                    raise SchemaTagAlreadyExistsError(
                        f"Tag '{tag_name}' already exists in the schema"
                    )

            session.commit()

    def remove_tag_from_schema(
        self, namespace: str, name: str, version: str, tag: str
    ) -> None:
        """Remove a tag from a schema version.

        Args:
            namespace: Namespace of the schema.
            name: Name of the schema.
            version: Schema version.
            tag: Name of the tag to remove.

        Raises:
            SchemaVersionDoesNotExistError: If the version does not exist.
            SchemaTagDoesNotExistError: If the tag does not exist on the version.
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
                    SchemaTags.tag_name == tag,
                    SchemaTags.schema_version_id == schema_obj.id,
                )
            )
            if not tag_obj:
                raise SchemaTagDoesNotExistError(
                    f"Tag '{tag}' does not exist in the schema"
                )

            session.delete(tag_obj)
            session.commit()

    @staticmethod
    def _add_order_by_schemas_keyword(
        statement: Select, by: str = "update_date", desc: bool = False
    ) -> Select:
        """Add an ORDER BY clause for schema queries.

        Args:
            statement: SQLAlchemy SELECT statement to augment.
            by: Sort field — "name" or "update_date".
            desc: Sort in descending order if True.

        Returns:
            Statement with ORDER BY applied.
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
        self, tags: list[str] | str | dict[str, str] | list[dict[str, str]] | None
    ) -> dict[str, str]:
        """Normalise tags to a dict[str, str] representation.

        Args:
            tags: Tags as a string, list of strings, dict, or list of dicts.

        Returns:
            Dict mapping tag names to tag values.

        Raises:
            ValueError: If tags are in an unsupported format.
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
