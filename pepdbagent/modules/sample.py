import datetime
import logging

import peprs
from sqlalchemy import and_, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from pepdbagent.const import DEFAULT_TAG, PKG_NAME, SAMPLE_TABLE_INDEX_KEY
from pepdbagent.db_utils import BaseEngine, Projects, Samples
from pepdbagent.exceptions import SampleAlreadyExistsError, SampleNotFoundError
from pepdbagent.utils import generate_guid, order_samples

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseSample:
    """
    Class that represents Project in Database.

    While using this class, user can create, retrieve, delete, and update projects from database
    """

    def __init__(self, pep_db_engine: BaseEngine):
        """
        Args:
            pep_db_engine: PEPDatabaseAgent engine object.
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def get(
        self,
        namespace: str,
        name: str,
        sample_name: str,
        tag: str = DEFAULT_TAG,
        raw: bool = True,
    ) -> peprs.Sample | dict | None:
        """Retrieve a sample from the database.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            sample_name: Name of the sample.
            tag: Tag of the project.
            raw: Return raw dict if True, peprs.Sample object if False.

        Returns:
            Raw sample dict or peprs.Sample object.

        Raises:
            SampleNotFoundError: If the sample does not exist.
        """
        statement_sample = select(Samples).where(
            and_(
                Samples.project_id
                == select(Projects.id)
                .where(
                    and_(
                        Projects.namespace == namespace,
                        Projects.name == name,
                        Projects.tag == tag,
                    ),
                )
                .scalar_subquery(),
                Samples.sample_name == sample_name,
            )
        )
        project_config_statement = select(Projects.config).where(
            and_(
                Projects.namespace == namespace,
                Projects.name == name,
                Projects.tag == tag,
            )
        )

        with Session(self._sa_engine) as session:
            result = session.scalar(statement_sample)
            if result:
                if not raw:
                    config = session.execute(project_config_statement).one_or_none()[0]
                    project = peprs.Project.from_dict(
                        pep_dictionary={
                            "config": config,
                            "samples": [result.sample],
                        }
                    )
                    return project.samples[0]
                else:
                    return result.sample
            else:
                raise SampleNotFoundError(
                    f"Sample {namespace}/{name}:{tag}?{sample_name} not found in the database"
                )

    def update(
        self,
        namespace: str,
        name: str,
        tag: str,
        sample_name: str,
        update_dict: dict,
        full_update: bool = False,
    ) -> None:
        """Update a sample in the database.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            sample_name: Name of the sample.
            update_dict: Dict of fields to update, e.g., {"sample_name": "s1", "protocol": "rna"}.
            full_update: Replace all sample fields if True, merge if False.

        Raises:
            SampleNotFoundError: If the sample does not exist.
        """
        statement = select(Samples).where(
            and_(
                Samples.project_id
                == select(Projects.id)
                .where(
                    and_(
                        Projects.namespace == namespace,
                        Projects.name == name,
                        Projects.tag == tag,
                    ),
                )
                .scalar_subquery(),
                Samples.sample_name == sample_name,
            )
        )
        project_statement = select(Projects).where(
            and_(
                Projects.namespace == namespace,
                Projects.name == name,
                Projects.tag == tag,
            )
        )
        with Session(self._sa_engine) as session:
            sample_mapping = session.scalar(statement)
            project_mapping = session.scalar(project_statement)

            if sample_mapping:
                if full_update:
                    sample_mapping.sample = update_dict
                else:
                    sample_mapping.sample.update(update_dict)
                try:
                    sample_mapping.sample_name = sample_mapping.sample[
                        project_mapping.config.get(
                            SAMPLE_TABLE_INDEX_KEY, "sample_name"
                        )
                    ]
                except KeyError:
                    raise KeyError(
                        f"Sample index key {project_mapping.config.get('sample_table_index', 'sample_name')} not found in sample dict"
                    )

                # This line needed due to: https://github.com/sqlalchemy/sqlalchemy/issues/5218
                flag_modified(sample_mapping, "sample")

                project_mapping.last_update_date = datetime.datetime.now(
                    datetime.timezone.utc
                )

                session.commit()
            else:
                raise SampleNotFoundError(
                    f"Sample {namespace}/{name}:{tag}?{sample_name} not found in the database"
                )

    def add(
        self,
        namespace: str,
        name: str,
        tag: str,
        sample_dict: dict,
        overwrite: bool = False,
    ) -> None:
        """Add a sample to a project in the database.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            sample_dict: Sample data, e.g., {"sample_name": "s1", "protocol": "rna"}.
            overwrite: Overwrite the sample if it already exists.

        Raises:
            SampleAlreadyExistsError: If the sample exists and overwrite is False.
        """

        with Session(self._sa_engine) as session:
            project_statement = select(Projects).where(
                and_(
                    Projects.namespace == namespace,
                    Projects.name == name,
                    Projects.tag == tag,
                )
            )
            # project mapping is needed to update number of samples, last_update_date and get sample_index_key
            project_mapping = session.scalar(project_statement)
            try:
                sample_name = sample_dict[
                    project_mapping.config.get(SAMPLE_TABLE_INDEX_KEY, "sample_name")
                ]
            except KeyError:
                raise KeyError(
                    f"Sample index key {project_mapping.config.get('sample_table_index', 'sample_name')} not found in sample dict"
                )
            statement = select(Samples).where(
                and_(
                    Samples.project_id == project_mapping.id,
                    Samples.sample_name == sample_name,
                )
            )
            sample_mapping = session.scalar(statement)

            if sample_mapping and not overwrite:
                raise SampleAlreadyExistsError(
                    f"Sample {namespace}/{name}:{tag}?{sample_name} already exists in the database"
                )
            elif sample_mapping and overwrite:
                self.update(
                    namespace=namespace,
                    name=name,
                    tag=tag,
                    sample_name=sample_name,
                    update_dict=sample_dict,
                    full_update=True,
                )
                return None
            else:
                sample_mapping = Samples(
                    sample=sample_dict,
                    project_id=project_mapping.id,
                    sample_name=sample_name,
                    guid=generate_guid(),
                    parent_guid=self._get_last_sample_guid(project_mapping.id),
                )
                project_mapping.number_of_samples += 1
                project_mapping.last_update_date = datetime.datetime.now(
                    datetime.timezone.utc
                )

                session.add(sample_mapping)
                session.commit()

    def _get_last_sample_guid(self, project_id: int) -> str:
        """Get the guid of the last sample in the project chain.

        Args:
            project_id: Database id of the project.

        Returns:
            GUID of the last sample.
        """
        statement = select(Samples).where(Samples.project_id == project_id)
        with Session(self._sa_engine) as session:
            samples_results = session.scalars(statement)

            result_dict = {}
            for sample in samples_results:
                sample_dict = sample.sample

                result_dict[sample.guid] = {
                    "sample": sample_dict,
                    "guid": sample.guid,
                    "parent_guid": sample.parent_guid,
                }
            return order_samples(result_dict)[-1]["guid"]

    def delete(
        self,
        namespace: str,
        name: str,
        tag: str,
        sample_name: str,
    ) -> None:
        """Delete a sample from the database.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            sample_name: Name of the sample.

        Raises:
            SampleNotFoundError: If the sample does not exist.
        """
        statement = select(Samples).where(
            and_(
                Samples.project_id
                == select(Projects.id)
                .where(
                    and_(
                        Projects.namespace == namespace,
                        Projects.name == name,
                        Projects.tag == tag,
                    ),
                )
                .scalar_subquery(),
                Samples.sample_name == sample_name,
            )
        )
        project_statement = select(Projects).where(
            and_(
                Projects.namespace == namespace,
                Projects.name == name,
                Projects.tag == tag,
            )
        )
        with Session(self._sa_engine) as session:
            sample_mapping = session.scalar(statement)
            project_mapping = session.scalar(project_statement)

            if sample_mapping:
                parent_mapping = sample_mapping.parent_mapping
                child_mapping = sample_mapping.child_mapping
                session.delete(sample_mapping)
                if child_mapping:
                    child_mapping.parent_mapping = parent_mapping
                project_mapping.number_of_samples -= 1
                project_mapping.last_update_date = datetime.datetime.now(
                    datetime.timezone.utc
                )
                session.commit()
            else:
                raise SampleNotFoundError(
                    f"Sample {namespace}/{name}:{tag}?{sample_name} not found in the database"
                )
