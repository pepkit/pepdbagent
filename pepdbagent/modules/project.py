import datetime
import json
import logging
from typing import Dict, List, NoReturn, Union

import numpy as np
import peppy
from peppy.const import (
    CONFIG_KEY,
    SAMPLE_NAME_ATTR,
    SAMPLE_RAW_DICT_KEY,
    SAMPLE_TABLE_INDEX_KEY,
    SUBSAMPLE_RAW_LIST_KEY,
)
from sqlalchemy import Select, and_, delete, select
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from pepdbagent.const import (
    DEFAULT_TAG,
    DESCRIPTION_KEY,
    MAX_HISTORY_SAMPLES_NUMBER,
    NAME_KEY,
    PEPHUB_SAMPLE_ID_KEY,
    PKG_NAME,
)
from pepdbagent.db_utils import (
    BaseEngine,
    HistoryProjects,
    HistorySamples,
    Projects,
    Samples,
    SchemaRecords,
    SchemaVersions,
    Subsamples,
    UpdateTypes,
    User,
)
from pepdbagent.exceptions import (
    HistoryNotFoundError,
    PEPDatabaseAgentError,
    ProjectDuplicatedSampleGUIDsError,
    ProjectNotFoundError,
    ProjectUniqueNameError,
    SampleTableUpdateError,
    SchemaDoesNotExistError,
)
from pepdbagent.models import (
    HistoryAnnotationModel,
    HistoryChangeModel,
    ProjectDict,
    UpdateItems,
    UpdateModel,
)
from pepdbagent.utils import (
    create_digest,
    generate_guid,
    order_samples,
    registry_path_converter,
    schema_path_converter,
)

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseProject:
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
        tag: str = DEFAULT_TAG,
        raw: bool = True,
        with_id: bool = False,
    ) -> Union[peppy.Project, dict, None]:
        """
        Retrieve project from database by specifying namespace, name and tag

        :param namespace: namespace of the project
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param raw: retrieve unprocessed (raw) PEP dict.
        :param with_id: retrieve project with id [default: False]
        :return: peppy.Project object with found project or dict with unprocessed
            PEP elements: {
                name: str
                description: str
                _config: dict
                _sample_dict: dict
                _subsample_dict: dict
            }
        """
        # name = name.lower()
        namespace = namespace.lower()
        statement = self._create_select_statement(name, namespace, tag)

        try:
            with Session(self._sa_engine) as session:
                found_prj = session.scalar(statement)

                if found_prj:
                    _LOGGER.info(
                        f"Project has been found: {found_prj.namespace}, {found_prj.name}"
                    )
                    subsample_dict = {}
                    if found_prj.subsamples_mapping:
                        for subsample in found_prj.subsamples_mapping:
                            if subsample.subsample_number not in subsample_dict.keys():
                                subsample_dict[subsample.subsample_number] = []
                            subsample_dict[subsample.subsample_number].append(subsample.subsample)
                        subsample_list = list(subsample_dict.values())
                    else:
                        subsample_list = []

                    sample_list = self._get_samples(
                        session=session, prj_id=found_prj.id, with_id=with_id
                    )

                    project_value = {
                        CONFIG_KEY: found_prj.config,
                        SAMPLE_RAW_DICT_KEY: sample_list,
                        SUBSAMPLE_RAW_LIST_KEY: subsample_list,
                    }

                    if raw:
                        return project_value
                    else:
                        project_obj = peppy.Project().from_dict(project_value)
                        return project_obj

                else:
                    raise ProjectNotFoundError(
                        f"No project found for supplied input: '{namespace}/{name}:{tag}'. "
                        f"Did you supply a valid namespace and project?"
                    )

        except NoResultFound:
            raise ProjectNotFoundError

    def _get_samples(self, session: Session, prj_id: int, with_id: bool) -> List[Dict]:
        """
        Get samples from the project. This method is used to retrieve samples from the project,
            with open session object.

        :param session: open session object
        :param prj_id: project id
        :param with_id: retrieve sample with id
        """
        result_dict = self._get_samples_dict(prj_id, session, with_id)

        result_dict = order_samples(result_dict)

        ordered_samples_list = [sample["sample"] for sample in result_dict]
        return ordered_samples_list

    @staticmethod
    def _get_samples_dict(prj_id: int, session: Session, with_id: bool) -> Dict:
        """
        Get not ordered samples from the project. This method is used to retrieve samples from the project

        :param prj_id: project id
        :param session: open session object
        :param with_id: retrieve sample with id

        :return: dictionary with samples:
            {guid:
                {
                    "sample": sample_dict,
                    "guid": guid,
                    "parent_guid": parent_guid
                }
            }
        """
        samples_results = session.scalars(select(Samples).where(Samples.project_id == prj_id))
        result_dict = {}
        for sample in samples_results:
            sample_dict = sample.sample
            if with_id:
                sample_dict[PEPHUB_SAMPLE_ID_KEY] = sample.guid

            result_dict[sample.guid] = {
                "sample": sample_dict,
                "guid": sample.guid,
                "parent_guid": sample.parent_guid,
            }

        return result_dict

    @staticmethod
    def _create_select_statement(name: str, namespace: str, tag: str = DEFAULT_TAG) -> Select:
        """
        Create simple select statement for retrieving project from database

        :param name: name of the project
        :param namespace: namespace of the project
        :param tag: tag of the project
        :return: select statement
        """
        statement = select(Projects)
        statement = statement.where(
            and_(
                Projects.namespace == namespace,
                Projects.name == name,
                Projects.tag == tag,
            )
        )
        return statement

    def get_by_rp(
        self,
        registry_path: str,
        raw: bool = False,
    ) -> Union[peppy.Project, dict, None]:
        """
        Retrieve project from database by specifying project registry_path

        :param registry_path: project registry_path [e.g. namespace/name:tag]
        :param raw: retrieve unprocessed (raw) PEP dict.
        :return: peppy.Project object with found project or dict with unprocessed
            PEP elements: {
                name: str
                description: str
                _config: dict
                _sample_dict: dict
                _subsample_dict: dict
            }
        """
        namespace, name, tag = registry_path_converter(registry_path)
        return self.get(namespace=namespace, name=name, tag=tag, raw=raw)

    def delete(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> None:
        """
        Delete record from database

        :param namespace: Namespace
        :param name: Name
        :param tag: Tag
        :return: None
        """
        # name = name.lower()
        namespace = namespace.lower()

        if not self.exists(namespace=namespace, name=name, tag=tag):
            raise ProjectNotFoundError(
                f"Can't delete unexciting project: '{namespace}/{name}:{tag}'."
            )

        with Session(self._sa_engine) as session:
            session.execute(
                delete(Projects).where(
                    and_(
                        Projects.namespace == namespace,
                        Projects.name == name,
                        Projects.tag == tag,
                    )
                )
            )

            statement = select(User).where(User.namespace == namespace)
            user = session.scalar(statement)
            if user:
                user.number_of_projects -= 1
                session.commit()

    def delete_by_rp(
        self,
        registry_path: str,
    ) -> None:
        """
        Delete record from database by using registry_path

        :param registry_path: Registry path of the project ('namespace/name:tag')
        :return: None
        """
        namespace, name, tag = registry_path_converter(registry_path)
        return self.delete(namespace=namespace, name=name, tag=tag)

    def create(
        self,
        project: Union[peppy.Project, dict],
        namespace: str,
        name: str = None,
        tag: str = DEFAULT_TAG,
        description: str = None,
        is_private: bool = False,
        pop: bool = False,
        pep_schema: str = None,
        overwrite: bool = False,
        update_only: bool = False,
    ) -> None:
        """
        Upload project to the database.
        Project with the key, that already exists won't be uploaded(but case, when argument
        update is set True)

        :param peppy.Project project: Project object that has to be uploaded to the DB
            danger zone:
                optionally, project can be a dictionary with PEP elements
                ({
                    _config: dict,
                    _sample_dict: Union[list, dict],
                    _subsample_list: list
                })
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param is_private: boolean value if the project should be visible just for user that creates it.
        :param pep_schema: assign PEP to a specific schema. Example: 'namespace/name' [Default: None]
        :param pop: if project is a pep of peps (POP) [Default: False]
        :param overwrite: if project exists overwrite the project, otherwise upload it.
            [Default: False - project won't be overwritten if it exists in db]
        :param update_only: if project exists overwrite it, otherwise do nothing.  [Default: False]
        :param description: description of the project
        :return: None
        """
        if isinstance(project, peppy.Project):
            proj_dict = project.to_dict(extended=True, orient="records")
        elif isinstance(project, dict):
            # verify if the dictionary has all necessary elements.
            # samples should be always presented as list of dicts (orient="records"))
            _LOGGER.warning(
                f"Project f{namespace}/{name}:{tag} is provided as dictionary. Project won't be validated."
            )
            proj_dict = ProjectDict(**project).model_dump(by_alias=True)
        else:
            raise PEPDatabaseAgentError(
                "Project has to be peppy.Project object or dictionary with PEP elements"
            )

        if not description:
            description = project.get(description, "")
        proj_dict[CONFIG_KEY][DESCRIPTION_KEY] = description

        namespace = namespace.lower()
        if name:
            proj_name = name.lower()
        elif proj_dict[CONFIG_KEY][NAME_KEY]:
            proj_name = proj_dict[CONFIG_KEY][NAME_KEY].lower()
        else:
            raise ValueError("Name of the project wasn't provided. Project will not be uploaded.")

        proj_dict[CONFIG_KEY][NAME_KEY] = proj_name

        proj_digest = create_digest(proj_dict)
        try:
            number_of_samples = len(project.samples)
        except AttributeError:
            number_of_samples = len(proj_dict[SAMPLE_RAW_DICT_KEY])

        if pep_schema:
            schema_namespace, schema_name, schema_version = schema_path_converter(pep_schema)
            with Session(self._sa_engine) as session:

                schema_mapping = session.scalar(
                    select(SchemaVersions)
                    .join(SchemaRecords, SchemaRecords.id == SchemaVersions.schema_id)
                    .where(
                        and_(
                            SchemaRecords.namespace == schema_namespace,
                            SchemaRecords.name == schema_name,
                            SchemaVersions.version == schema_version,
                        )
                    )
                )
                if not schema_mapping:
                    raise SchemaDoesNotExistError(
                        f"Schema {schema_namespace}/{schema_name} does not exist. "
                        f"Project won't be uploaded."
                    )
                pep_schema = schema_mapping.id

        if update_only:
            _LOGGER.info(f"Update_only argument is set True. Updating project {proj_name} ...")
            self._overwrite(
                project_dict=proj_dict,
                namespace=namespace,
                proj_name=proj_name,
                tag=tag,
                project_digest=proj_digest,
                number_of_samples=number_of_samples,
                private=is_private,
                pep_schema=pep_schema,
                description=description,
                pop=pop,
            )
            return None
        else:
            try:
                _LOGGER.info(f"Uploading {namespace}/{proj_name}:{tag} project...")
                new_prj = Projects(
                    namespace=namespace,
                    name=proj_name,
                    tag=tag,
                    digest=proj_digest,
                    config=proj_dict[CONFIG_KEY],
                    number_of_samples=number_of_samples,
                    private=is_private,
                    submission_date=datetime.datetime.now(datetime.timezone.utc),
                    last_update_date=datetime.datetime.now(datetime.timezone.utc),
                    # pep_schema=pep_schema,
                    schema_id=pep_schema,
                    description=description,
                    pop=pop,
                )

                self._add_samples_to_project(
                    new_prj,
                    proj_dict[SAMPLE_RAW_DICT_KEY],
                    sample_table_index=proj_dict[CONFIG_KEY].get(
                        SAMPLE_TABLE_INDEX_KEY, SAMPLE_NAME_ATTR
                    ),
                )

                if proj_dict[SUBSAMPLE_RAW_LIST_KEY]:
                    subsamples = proj_dict[SUBSAMPLE_RAW_LIST_KEY]
                    self._add_subsamples_to_project(new_prj, subsamples)

                with Session(self._sa_engine) as session:
                    user = session.scalar(select(User).where(User.namespace == namespace))

                    if not user:
                        user = User(namespace=namespace)
                        session.add(user)
                        session.commit()

                    user.number_of_projects += 1

                    session.add(new_prj)
                    session.commit()

                return None

            except IntegrityError:
                if overwrite:
                    self._overwrite(
                        project_dict=proj_dict,
                        namespace=namespace,
                        proj_name=proj_name,
                        tag=tag,
                        project_digest=proj_digest,
                        number_of_samples=number_of_samples,
                        private=is_private,
                        pep_schema=pep_schema,
                        description=description,
                    )
                    return None

                else:
                    raise ProjectUniqueNameError(
                        "Namespace, name and tag already exists. Project won't be "
                        "uploaded. Solution: Set overwrite value as True"
                        " (project will be overwritten), or change tag!"
                    )

    def _overwrite(
        self,
        project_dict: json,
        namespace: str,
        proj_name: str,
        tag: str,
        project_digest: str,
        number_of_samples: int,
        private: bool = False,
        pep_schema: int = None,
        description: str = "",
        pop: bool = False,
    ) -> None:
        """
        Update existing project by providing all necessary information.

        :param project_dict: project dictionary in json format
        :param namespace: project namespace
        :param proj_name: project name
        :param tag: project tag
        :param project_digest: project digest
        :param number_of_samples: number of samples in project
        :param private: boolean value if the project should be visible just for user that creates it.
        :param pep_schema: assign PEP to a specific schema. [DefaultL: None]
        :param description: project description
        :param pop: if project is a pep of peps, simply POP [Default: False]
        :return: None
        """
        proj_name = proj_name.lower()
        namespace = namespace.lower()
        if self.exists(namespace=namespace, name=proj_name, tag=tag):
            _LOGGER.info(f"Updating {proj_name} project...")
            statement = self._create_select_statement(proj_name, namespace, tag)

            with Session(self._sa_engine) as session:
                found_prj = session.scalar(statement)

                if found_prj:
                    _LOGGER.debug(
                        f"Project has been found: {found_prj.namespace}, {found_prj.name}"
                    )

                    found_prj.digest = project_digest
                    found_prj.number_of_samples = number_of_samples
                    found_prj.private = private
                    # found_prj.pep_schema = pep_schema
                    found_prj.schema_id = pep_schema
                    found_prj.config = project_dict[CONFIG_KEY]
                    found_prj.description = description
                    found_prj.last_update_date = datetime.datetime.now(datetime.timezone.utc)
                    found_prj.pop = pop

                    # Deleting old samples and subsamples
                    if found_prj.samples_mapping:
                        for sample in found_prj.samples_mapping:
                            _LOGGER.debug(f"deleting samples: {str(sample)}")
                            session.delete(sample)

                    if found_prj.subsamples_mapping:
                        for subsample in found_prj.subsamples_mapping:
                            _LOGGER.debug(f"deleting subsamples: {str(subsample)}")
                            session.delete(subsample)

                # Adding new samples and subsamples
                self._add_samples_to_project(
                    found_prj,
                    project_dict[SAMPLE_RAW_DICT_KEY],
                    sample_table_index=project_dict[CONFIG_KEY].get(SAMPLE_TABLE_INDEX_KEY),
                )

                if project_dict[SUBSAMPLE_RAW_LIST_KEY]:
                    self._add_subsamples_to_project(
                        found_prj, project_dict[SUBSAMPLE_RAW_LIST_KEY]
                    )

                session.commit()

            _LOGGER.info(f"Project '{namespace}/{proj_name}:{tag}' has been successfully updated!")
            return None

        else:
            raise ProjectNotFoundError("Project does not exist! No project will be updated!")

    def update(
        self,
        update_dict: Union[dict, UpdateItems],
        namespace: str,
        name: str,
        tag: str = DEFAULT_TAG,
        user: str = None,
    ) -> None:
        """
        Update partial parts of the record in db

        :param update_dict: dict with update key->values. Dict structure:
            {
                    project: Optional[peppy.Project]
                    is_private: Optional[bool]
                    tag: Optional[str]
                    name: Optional[str]
                    description: Optional[str]
                    is_private: Optional[bool]
                    pep_schema: Optional[str]
                    config: Optional[dict]
                    samples: Optional[List[dict]]
                    subsamples: Optional[List[List[dict]]]
                    pop: Optional[bool]
            }
        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :param user: user that updates the project if user is not provided, user will be set as Namespace
        :return: None
        """
        if self.exists(namespace=namespace, name=name, tag=tag):
            if isinstance(update_dict, UpdateItems):
                update_values = update_dict
            else:
                if "project" in update_dict:
                    project_dict = update_dict.pop("project").to_dict(
                        extended=True, orient="records"
                    )
                    update_dict["config"] = project_dict[CONFIG_KEY]
                    update_dict["samples"] = project_dict[SAMPLE_RAW_DICT_KEY]
                    update_dict["subsamples"] = project_dict[SUBSAMPLE_RAW_LIST_KEY]

                update_values = UpdateItems(**update_dict)

            update_values = self.__create_update_dict(update_values)

            statement = self._create_select_statement(name, namespace, tag)

            with Session(self._sa_engine) as session:
                found_prj: Projects = session.scalar(statement)

                if not found_prj:
                    raise ProjectNotFoundError(
                        f"Pep {namespace}/{name}:{tag} was not found. No items will be updated!"
                    )

                self._convert_update_schema_id(session, update_values)

                for k, v in update_values.items():
                    if getattr(found_prj, k) != v:
                        setattr(found_prj, k, v)

                        # standardizing project name
                        if k == NAME_KEY:
                            if "config" in update_values:
                                update_values["config"][NAME_KEY] = v
                            else:
                                found_prj.config[NAME_KEY] = v
                                flag_modified(found_prj, "config")
                            found_prj.name = found_prj.config[NAME_KEY]

                        if k == DESCRIPTION_KEY:
                            if "config" in update_values:
                                update_values["config"][DESCRIPTION_KEY] = v
                            else:
                                found_prj.config[DESCRIPTION_KEY] = v
                                # This line needed due to: https://github.com/sqlalchemy/sqlalchemy/issues/5218
                                flag_modified(found_prj, "config")

                if "samples" in update_dict:

                    if PEPHUB_SAMPLE_ID_KEY not in update_dict["samples"][0]:
                        raise SampleTableUpdateError(
                            f"pephub_sample_id '{PEPHUB_SAMPLE_ID_KEY}' is missing in samples."
                            f"Please provide it to update samples, or use overwrite method."
                        )
                    if len(update_dict["samples"]) > MAX_HISTORY_SAMPLES_NUMBER:
                        _LOGGER.warning(
                            f"Number of samples in the project exceeds the limit of {MAX_HISTORY_SAMPLES_NUMBER}."
                            f"Samples won't be updated."
                        )
                        new_history = None
                    else:
                        new_history = HistoryProjects(
                            project_id=found_prj.id,
                            user=user or namespace,
                            project_yaml=self.get_config(namespace, name, tag),
                        )
                        session.add(new_history)

                    self._update_samples(
                        project_id=found_prj.id,
                        samples_list=update_dict["samples"],
                        sample_name_key=update_dict["config"].get(
                            SAMPLE_TABLE_INDEX_KEY, "sample_name"
                        ),
                        history_sa_model=new_history,
                    )
                    found_prj.number_of_samples = len(update_dict["samples"])

                if "subsamples" in update_dict:
                    if found_prj.subsamples_mapping:
                        for subsample in found_prj.subsamples_mapping:
                            _LOGGER.debug(f"deleting subsamples: {str(subsample)}")
                            session.delete(subsample)

                    # Adding new subsamples
                    if update_dict["subsamples"]:
                        self._add_subsamples_to_project(found_prj, update_dict["subsamples"])

                found_prj.last_update_date = datetime.datetime.now(datetime.timezone.utc)

                session.commit()

            return None

        else:
            raise ProjectNotFoundError("No items will be updated!")

    @staticmethod
    def _convert_update_schema_id(session: Session, update_values: dict):
        """
        Convert schema path to schema_id in update_values and update it in update dict


        :param session: open session object
        :param update_values: dict with update key->values

        return None
        """
        if "pep_schema" in update_values:
            schema_namespace, schema_name, schema_version = schema_path_converter(
                update_values["pep_schema"]
            )
            where_clause = and_(
                SchemaRecords.namespace == schema_namespace,
                SchemaRecords.name == schema_name,
                SchemaVersions.version == schema_version,
            )

            schema_mapping = session.scalar(
                select(SchemaVersions).join(SchemaRecords).where(where_clause)
            )
            if not schema_mapping:
                raise SchemaDoesNotExistError(
                    f"Schema {schema_namespace}/{schema_name} does not exist. "
                    f"Project won't be updated."
                )
            update_values["schema_id"] = schema_mapping.id
            del update_values["pep_schema"]

    def _update_samples(
        self,
        project_id: int,
        samples_list: List[Dict[str, str]],
        sample_name_key: str = "sample_name",
        history_sa_model: Union[HistoryProjects, None] = None,
    ) -> None:
        """
        Update samples in the project
        This is linked list method, that first finds differences in old and new samples list
            and then updates, adds, inserts, deletes, or changes the order.

        :param project_id: project id in PEPhub database
        :param samples_list: list of samples to be updated
        :param sample_name_key: key of the sample name
        :param history_sa_model: HistoryProjects object, to write to the history table
        :return: None
        """

        with Session(self._sa_engine) as session:
            old_samples = session.scalars(select(Samples).where(Samples.project_id == project_id))

            old_samples_mapping: dict = {sample.guid: sample for sample in old_samples}

            # old_child_parent_id needed because of the parent_guid is sometimes set to none in sqlalchemy mapping :( bug
            old_child_parent_id: Dict[str, str] = {
                child: mapping.parent_guid for child, mapping in old_samples_mapping.items()
            }

            old_samples_ids_set: set = set(old_samples_mapping.keys())
            new_samples_ids_list: list = [
                new_sample[PEPHUB_SAMPLE_ID_KEY]
                for new_sample in samples_list
                if new_sample[PEPHUB_SAMPLE_ID_KEY] != ""
                and new_sample[PEPHUB_SAMPLE_ID_KEY] is not None
            ]
            new_samples_ids_set: set = set(new_samples_ids_list)
            new_samples_dict: dict = {
                new_sample[PEPHUB_SAMPLE_ID_KEY] or generate_guid(): new_sample
                for new_sample in samples_list
            }

            if len(new_samples_ids_list) != len(new_samples_ids_set):
                raise ProjectDuplicatedSampleGUIDsError(
                    f"Samples have to have unique pephub_sample_id: '{PEPHUB_SAMPLE_ID_KEY}'."
                    f"If ids are duplicated, overwrite the project."
                )

            # Check if something was deleted:
            deleted_ids = old_samples_ids_set - new_samples_ids_set

            del new_samples_ids_list, new_samples_ids_set

            for remove_id in deleted_ids:

                if history_sa_model:
                    history_sa_model.sample_changes_mapping.append(
                        HistorySamples(
                            guid=old_samples_mapping[remove_id].guid,
                            parent_guid=old_child_parent_id[remove_id],
                            sample_json=old_samples_mapping[remove_id].sample,
                            change_type=UpdateTypes.DELETE,
                        )
                    )
                session.delete(old_samples_mapping[remove_id])

            parent_id = None
            parent_mapping = None

            # Main loop to update samples
            for current_id, sample_value in new_samples_dict.items():
                new_sample = None
                del sample_value[PEPHUB_SAMPLE_ID_KEY]

                if current_id not in old_samples_ids_set:
                    new_sample = Samples(
                        sample=sample_value,
                        guid=current_id,
                        sample_name=sample_value[sample_name_key],
                        project_id=project_id,
                        parent_mapping=parent_mapping,
                    )
                    session.add(new_sample)

                    if history_sa_model:
                        history_sa_model.sample_changes_mapping.append(
                            HistorySamples(
                                guid=new_sample.guid,
                                parent_guid=new_sample.parent_guid,
                                sample_json=new_sample.sample,
                                change_type=UpdateTypes.INSERT,
                            )
                        )

                else:
                    current_history = None
                    if old_samples_mapping[current_id].sample != sample_value:

                        if history_sa_model:
                            current_history = HistorySamples(
                                guid=old_samples_mapping[current_id].guid,
                                parent_guid=old_samples_mapping[current_id].parent_guid,
                                sample_json=old_samples_mapping[current_id].sample,
                                change_type=UpdateTypes.UPDATE,
                            )

                        old_samples_mapping[current_id].sample = sample_value
                        old_samples_mapping[current_id].sample_name = sample_value[sample_name_key]

                    # !bug workaround: if project was deleted and sometimes old_samples_mapping[current_id].parent_guid
                    # and it can cause an error in history. For this we have `old_child_parent_id` dict
                    if old_samples_mapping[current_id].parent_guid != parent_id:
                        if history_sa_model:
                            if current_history:
                                current_history.parent_guid = parent_id
                            else:
                                current_history = HistorySamples(
                                    guid=old_samples_mapping[current_id].guid,
                                    parent_guid=old_child_parent_id[current_id],
                                    sample_json=old_samples_mapping[current_id].sample,
                                    change_type=UpdateTypes.UPDATE,
                                )
                        old_samples_mapping[current_id].parent_mapping = parent_mapping

                    if history_sa_model and current_history:
                        history_sa_model.sample_changes_mapping.append(current_history)

                parent_id = current_id
                parent_mapping = new_sample or old_samples_mapping[current_id]

            session.commit()

    @staticmethod
    def __create_update_dict(update_values: UpdateItems) -> dict:
        """
        Modify keys and values that set for update and create unified
        dictionary of the values that have to be updated

         :param update_values: UpdateItems (pydantic class) with
            updating values
        :return: unified update dict
        """
        update_final = UpdateModel.model_construct()

        if update_values.name is not None:
            if update_values.config is not None:
                update_values.config[NAME_KEY] = update_values.name
            update_final = UpdateModel(
                name=update_values.name,
                **update_final.model_dump(exclude_unset=True),
            )

        if update_values.description is not None:
            if update_values.config is not None:
                update_values.config[DESCRIPTION_KEY] = update_values.description
            update_final = UpdateModel(
                description=update_values.description,
                **update_final.model_dump(exclude_unset=True),
            )
        if update_values.config is not None:
            update_final = UpdateModel(
                config=update_values.config, **update_final.model_dump(exclude_unset=True)
            )
            name = update_values.config.get(NAME_KEY)
            description = update_values.config.get(DESCRIPTION_KEY)
            if name:
                update_final = UpdateModel(
                    name=name,
                    **update_final.model_dump(exclude_unset=True, exclude={NAME_KEY}),
                )
            if description:
                update_final = UpdateModel(
                    description=description,
                    **update_final.model_dump(exclude_unset=True, exclude={DESCRIPTION_KEY}),
                )

        if update_values.tag is not None:
            update_final = UpdateModel(
                tag=update_values.tag, **update_final.model_dump(exclude_unset=True)
            )

        if update_values.is_private is not None:
            update_final = UpdateModel(
                is_private=update_values.is_private,
                **update_final.model_dump(exclude_unset=True),
            )
        if update_values.pop is not None:
            update_final = UpdateModel(
                pop=update_values.pop,
                **update_final.model_dump(exclude_unset=True),
            )

        if update_values.pep_schema is not None:
            update_final = UpdateModel(
                pep_schema=update_values.pep_schema,
                **update_final.model_dump(exclude_unset=True),
            )

        if update_values.number_of_samples is not None:
            update_final = UpdateModel(
                number_of_samples=update_values.number_of_samples,
                **update_final.model_dump(exclude_unset=True),
            )

        return update_final.model_dump(exclude_unset=True, exclude_none=True)

    def exists(
        self,
        namespace: str,
        name: str,
        tag: str = DEFAULT_TAG,
    ) -> bool:
        """
        Check if project exists in the database.
        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :return: Returning True if project exist
        """

        statement = select(Projects.id)
        statement = statement.where(
            and_(
                Projects.namespace == namespace,
                Projects.name == name,
                Projects.tag == tag,
            )
        )
        found_prj = self._pep_db_engine.session_execute(statement).all()

        if len(found_prj) > 0:
            return True
        else:
            return False

    @staticmethod
    def _add_samples_to_project(
        projects_sa: Projects, samples: List[dict], sample_table_index: str = "sample_name"
    ) -> None:
        """
        Add samples to the project sa object. (With commit this samples will be added to the 'samples table')
        :param projects_sa: Projects sa object, in open session
        :param samples: list of samles to be added to the database
        :param sample_table_index: index of the sample table
        :return: NoReturn
        """
        previous_sample_guid = None
        for sample in samples:

            sample = Samples(
                sample=sample,
                sample_name=sample.get(sample_table_index),
                parent_guid=previous_sample_guid,
                guid=generate_guid(),
            )
            projects_sa.samples_mapping.append(sample)
            previous_sample_guid = sample.guid

        return None

    @staticmethod
    def _add_subsamples_to_project(
        projects_sa: Projects, subsamples: List[List[dict]]
    ) -> NoReturn:
        """
        Add subsamples to the project sa object. (With commit this samples will be added to the 'subsamples table')

        :param projects_sa: Projects sa object, in open session
        :param subsamples: list of subsamles to be added to the database
        :return: NoReturn
        """
        for i, subs in enumerate(subsamples):
            for row_number, sub_item in enumerate(subs):
                projects_sa.subsamples_mapping.append(
                    Subsamples(subsample=sub_item, subsample_number=i, row_number=row_number)
                )

    def get_project_id(self, namespace: str, name: str, tag: str) -> Union[int, None]:
        """
        Get Project id by providing namespace, name, and tag

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :return: projects id
        """
        statement = select(Projects.id).where(
            and_(Projects.namespace == namespace, Projects.name == name, Projects.tag == tag)
        )
        with Session(self._sa_engine) as session:
            result = session.execute(statement).one_or_none()

        if result:
            return result[0]
        return None

    def fork(
        self,
        original_namespace: str,
        original_name: str,
        original_tag: str,
        fork_namespace: str,
        fork_name: str = None,
        fork_tag: str = None,
        description: str = None,
        private: bool = False,
    ) -> None:
        """
        Fork project from one namespace to another

        :param original_namespace: namespace of the project to be forked
        :param original_name: name of the project to be forked
        :param original_tag: tag of the project to be forked
        :param fork_namespace: namespace of the forked project
        :param fork_name: name of the forked project
        :param fork_tag: tag of the forked project
        :param description: description of the forked project
        :param private: boolean value if the project should be visible just for user that creates it.
        :return: None
        """

        self.create(
            project=self.get(
                namespace=original_namespace,
                name=original_name,
                tag=original_tag,
                raw=True,
            ),
            namespace=fork_namespace,
            name=fork_name,
            tag=fork_tag,
            description=description or None,
            is_private=private,
        )
        original_statement = select(Projects).where(
            and_(
                Projects.namespace == original_namespace,
                Projects.name == original_name,
                Projects.tag == original_tag,
            )
        )
        fork_statement = select(Projects).where(
            and_(
                Projects.namespace == fork_namespace,
                Projects.name == fork_name,
                Projects.tag == fork_tag,
            )
        )

        with Session(self._sa_engine) as session:
            original_prj = session.scalar(original_statement)
            fork_prj = session.scalar(fork_statement)

            fork_prj.forked_from_id = original_prj.id
            fork_prj.pop = original_prj.pop
            fork_prj.submission_date = original_prj.submission_date
            fork_prj.schema_id = original_prj.schema_id
            fork_prj.description = description or original_prj.description

            session.commit()

    def get_config(self, namespace: str, name: str, tag: str) -> Union[dict, None]:
        """
        Get project configuration by providing namespace, name, and tag

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :return: project configuration
        """
        statement = select(Projects.config).where(
            and_(Projects.namespace == namespace, Projects.name == name, Projects.tag == tag)
        )
        with Session(self._sa_engine) as session:
            result = session.execute(statement).one_or_none()

        if result:
            return result[0]
        return None

    def get_subsamples(self, namespace: str, name: str, tag: str) -> Union[list, None]:
        """
        Get project subsamples by providing namespace, name, and tag

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :return: list with project subsamples
        """
        statement = self._create_select_statement(name, namespace, tag)

        with Session(self._sa_engine) as session:

            found_prj = session.scalar(statement)

            if found_prj:
                _LOGGER.info(f"Project has been found: {found_prj.namespace}, {found_prj.name}")
                subsample_dict = {}
                if found_prj.subsamples_mapping:
                    for subsample in found_prj.subsamples_mapping:
                        if subsample.subsample_number not in subsample_dict.keys():
                            subsample_dict[subsample.subsample_number] = []
                        subsample_dict[subsample.subsample_number].append(subsample.subsample)
                    return list(subsample_dict.values())
                else:
                    return []
            else:
                raise ProjectNotFoundError(
                    f"No project found for supplied input: '{namespace}/{name}:{tag}'. "
                    f"Did you supply a valid namespace and project?"
                )

    def get_samples(
        self, namespace: str, name: str, tag: str, raw: bool = True, with_ids: bool = False
    ) -> list:
        """
        Get project samples by providing namespace, name, and tag

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :param raw: if True, retrieve unprocessed (raw) PEP dict. [Default: True]
        :param with_ids: if True, retrieve samples with ids. [Default: False]

        :return: list with project samples
        """
        if raw:
            return self.get(
                namespace=namespace, name=name, tag=tag, raw=True, with_id=with_ids
            ).get(SAMPLE_RAW_DICT_KEY)
        return (
            self.get(namespace=namespace, name=name, tag=tag, raw=False, with_id=with_ids)
            .sample_table.replace({np.nan: None})
            .to_dict(orient="records")
        )

    def get_history(self, namespace: str, name: str, tag: str) -> HistoryAnnotationModel:
        """
        Get project history annotation by providing namespace, name, and tag

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag

        :return: project history annotation
        """

        with Session(self._sa_engine) as session:
            statement = (
                select(HistoryProjects)
                .where(
                    HistoryProjects.project_id
                    == select(Projects.id)
                    .where(
                        and_(
                            Projects.namespace == namespace,
                            Projects.name == name,
                            Projects.tag == tag,
                        )
                    )
                    .scalar_subquery()
                )
                .order_by(HistoryProjects.update_time.desc())
            )
            results = session.scalars(statement)
            return_results: List = []

            if results:
                for result in results:
                    return_results.append(
                        HistoryChangeModel(
                            change_id=result.id,
                            change_date=result.update_time,
                            user=result.user,
                        )
                    )
            return HistoryAnnotationModel(
                namespace=namespace,
                name=name,
                tag=tag,
                history=return_results,
            )

    def get_project_from_history(
        self,
        namespace: str,
        name: str,
        tag: str,
        history_id: int,
        raw: bool = True,
        with_id: bool = False,
    ) -> Union[dict, peppy.Project]:
        """
        Get project sample history annotation by providing namespace, name, and tag

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :param history_id: history id
        :param raw: if True, retrieve unprocessed (raw) PEP dict. [Default: True]
        :param with_id: if True, retrieve samples with ids. [Default: False]

        :return: project sample history annotation
        """

        with Session(self._sa_engine) as session:
            project_mapping = session.scalar(
                select(Projects).where(
                    and_(
                        Projects.namespace == namespace,
                        Projects.name == name,
                        Projects.tag == tag,
                    )
                )
            )
            if not project_mapping:
                raise ProjectNotFoundError(
                    f"No project found for supplied input: '{namespace}/{name}:{tag}'. "
                    f"Did you supply a valid namespace and project?"
                )

            sample_dict = self._get_samples_dict(
                prj_id=project_mapping.id, session=session, with_id=True
            )

            main_history = session.scalar(
                select(HistoryProjects)
                .where(
                    and_(
                        HistoryProjects.project_id == project_mapping.id,
                        HistoryProjects.id == history_id,
                    )
                )
                .order_by(HistoryProjects.update_time.desc())
            )
            if not main_history:
                raise HistoryNotFoundError(
                    f"No history found for supplied input: '{namespace}/{name}:{tag}'. "
                    f"Did you supply a valid history id?"
                )

            changes_mappings = session.scalars(
                select(HistoryProjects)
                .where(
                    and_(
                        HistoryProjects.project_id == project_mapping.id,
                    )
                )
                .order_by(HistoryProjects.update_time.desc())
            )

            # Changes mapping is a ordered list from most early to latest changes
            # We have to loop through each change and apply it to the sample list
            # It should be done before we found the history_id that user is looking for
            project_config = None

            for result in changes_mappings:
                sample_dict = self._apply_history_changes(sample_dict, result)

                if result.id == history_id:
                    project_config = result.project_yaml
                    break

        samples_list = order_samples(sample_dict)
        ordered_samples_list = [sample["sample"] for sample in samples_list]

        if not with_id:
            for sample in ordered_samples_list:
                try:
                    del sample[PEPHUB_SAMPLE_ID_KEY]
                except KeyError:
                    pass

        if raw:
            return {
                CONFIG_KEY: project_config or project_mapping.config,
                SAMPLE_RAW_DICT_KEY: ordered_samples_list,
                SUBSAMPLE_RAW_LIST_KEY: self.get_subsamples(namespace, name, tag),
            }
        return peppy.Project.from_dict(
            pep_dictionary={
                CONFIG_KEY: project_config or project_mapping.config,
                SAMPLE_RAW_DICT_KEY: ordered_samples_list,
                SUBSAMPLE_RAW_LIST_KEY: self.get_subsamples(namespace, name, tag),
            }
        )

    @staticmethod
    def _apply_history_changes(sample_dict: dict, change: HistoryProjects) -> dict:
        """
        Apply changes from the history to the sample list

        :param sample_dict: dictionary with samples
        :param change: history change
        :return: updated sample list
        """
        for sample_change in change.sample_changes_mapping:
            sample_id = sample_change.guid

            if sample_change.change_type == UpdateTypes.UPDATE:
                sample_dict[sample_id]["sample"] = sample_change.sample_json
                sample_dict[sample_id]["sample"][PEPHUB_SAMPLE_ID_KEY] = sample_change.guid
                sample_dict[sample_id]["parent_guid"] = sample_change.parent_guid

            elif sample_change.change_type == UpdateTypes.DELETE:
                sample_dict[sample_id] = {
                    "sample": sample_change.sample_json,
                    "guid": sample_id,
                    "parent_guid": sample_change.parent_guid,
                }

            elif sample_change.change_type == UpdateTypes.INSERT:
                del sample_dict[sample_id]

        return sample_dict

    def delete_history(
        self, namespace: str, name: str, tag: str, history_id: Union[int, None] = None
    ) -> None:
        """
        Delete history from the project

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :param history_id: history id. If none is provided, all history will be deleted

        :return: None
        """
        with Session(self._sa_engine) as session:
            project_mapping = session.scalar(
                select(Projects).where(
                    and_(
                        Projects.namespace == namespace,
                        Projects.name == name,
                        Projects.tag == tag,
                    )
                )
            )
            if not project_mapping:
                raise ProjectNotFoundError(
                    f"No project found for supplied input: '{namespace}/{name}:{tag}'. "
                    f"Did you supply a valid namespace and project?"
                )

            if history_id is None:
                session.execute(
                    delete(HistoryProjects).where(HistoryProjects.project_id == project_mapping.id)
                )
                session.commit()
                return None

            history_mapping = session.scalar(
                select(HistoryProjects).where(
                    and_(
                        HistoryProjects.project_id == project_mapping.id,
                        HistoryProjects.id == history_id,
                    )
                )
            )
            if not history_mapping:
                raise HistoryNotFoundError(
                    f"No history found for supplied input: '{namespace}/{name}:{tag}'. "
                    f"Did you supply a valid history id?"
                )

            session.delete(history_mapping)
            session.commit()

    def restore(
        self,
        namespace: str,
        name: str,
        tag: str,
        history_id: int,
        user: str = None,
    ) -> None:
        """
        Restore project to the specific history state

        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :param history_id: history id
        :param user: user that restores the project if user is not provided, user will be set as Namespace

        :return: None
        """

        restore_project = self.get_project_from_history(
            namespace=namespace,
            name=name,
            tag=tag,
            history_id=history_id,
            raw=True,
            with_id=True,
        )
        self.update(
            update_dict={"project": peppy.Project.from_dict(restore_project)},
            namespace=namespace,
            name=name,
            tag=tag,
            user=user or namespace,
        )

    def clean_history(self, days: int = 90) -> None:
        """
        Delete all history data that is older than 3 month, or specific number of days

        :param days: number of days to keep history data
        :return: None
        """

        with Session(self._sa_engine) as session:
            session.execute(
                delete(HistoryProjects).where(
                    HistoryProjects.update_time
                    < (datetime.datetime.now() - datetime.timedelta(days=days))
                )
            )
            session.commit()
            _LOGGER.info("History was cleaned successfully!")
