import datetime
import logging
from typing import Union

import peppy
from peppy.const import SAMPLE_TABLE_INDEX_KEY
from sqlalchemy import and_, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from pepdbagent.const import DEFAULT_TAG, PKG_NAME
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
        :param pep_db_engine: pepdbengine object with sa engine
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
    ) -> Union[peppy.Sample, dict, None]:
        """
        Retrieve sample from the database using namespace, name, tag, and sample_name

        :param namespace: namespace of the project
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param sample_name: sample_name of the sample
        :param raw: return raw dict or peppy.Sample object [Default: True]
        :return: peppy.Project object with found project or dict with unprocessed
            PEP elements: {
                name: str
                description: str
                _config: dict
                _sample_dict: dict
                _subsample_dict: dict
            }
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
                    project = peppy.Project().from_dict(
                        pep_dictionary={
                            "name": name,
                            "description": config.get("description"),
                            "_config": config,
                            "_sample_dict": [result.sample],
                            "_subsample_dict": None,
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
        """
        Update one sample in the database

        :param namespace: namespace of the project
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param sample_name: sample_name of the sample
        :param update_dict: dictionary with sample data (key: value pairs). e.g.
            {"sample_name": "sample1",
            "sample_protocol": "sample1 protocol"}
        :param full_update: if True, update all sample fields, if False, update only fields from update_dict
        :return: None
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
                        project_mapping.config.get(SAMPLE_TABLE_INDEX_KEY, "sample_name")
                    ]
                except KeyError:
                    raise KeyError(
                        f"Sample index key {project_mapping.config.get(SAMPLE_TABLE_INDEX_KEY, 'sample_name')} not found in sample dict"
                    )

                # This line needed due to: https://github.com/sqlalchemy/sqlalchemy/issues/5218
                flag_modified(sample_mapping, "sample")

                project_mapping.last_update_date = datetime.datetime.now(datetime.timezone.utc)

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
        """
        Add one sample to the project in the database

        :param namespace: namespace of the project
        :param name: name of the project
        :param tag: tag (or version) of the project.
        :param overwrite: overwrite sample if it already exists
        :param sample_dict: dictionary with sample data (key: value pairs). e.g.
            {"sample_name": "sample1",
            "sample_protocol": "sample1 protocol"}
        :return: None
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
                    f"Sample index key {project_mapping.config.get(SAMPLE_TABLE_INDEX_KEY, 'sample_name')} not found in sample dict"
                )
            statement = select(Samples).where(
                and_(Samples.project_id == project_mapping.id, Samples.sample_name == sample_name)
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
                project_mapping.last_update_date = datetime.datetime.now(datetime.timezone.utc)

                session.add(sample_mapping)
                session.commit()

    def _get_last_sample_guid(self, project_id: int) -> str:
        """
        Get last sample guid from the project

        :param project_id: project_id of the project
        :return: guid of the last sample
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
        """
        Delete one sample from the database

        :param namespace: namespace of the project
        :param name: name of the project
        :param tag: tag (or version) of the project.
        :param sample_name: sample_name of the sample
        :return: None
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
                project_mapping.last_update_date = datetime.datetime.now(datetime.timezone.utc)
                session.commit()
            else:
                raise SampleNotFoundError(
                    f"Sample {namespace}/{name}:{tag}?{sample_name} not found in the database"
                )
