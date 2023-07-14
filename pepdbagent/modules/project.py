import datetime
import json
import logging
from typing import Union, List, NoReturn

import peppy
from sqlalchemy import Engine, and_, delete, insert, or_, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy import Select

from peppy.const import SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_LIST_KEY, CONFIG_KEY

from pepdbagent.const import *
from pepdbagent.db_utils import Projects, Samples, Subsamples, BaseEngine
from pepdbagent.exceptions import ProjectNotFoundError, ProjectUniqueNameError
from pepdbagent.models import UpdateItems, UpdateModel
from pepdbagent.utils import create_digest, registry_path_converter


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
        raw: bool = False,
    ) -> Union[peppy.Project, dict, None]:
        """
        Retrieve project from database by specifying namespace, name and tag

        :param namespace: namespace of the project
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
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
        # name = name.lower()
        namespace = namespace.lower()
        statement = self._create_select_statement(name, namespace, tag)

        try:
            with Session(self._sa_engine) as session:
                found_prj = session.scalars(statement).one()

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
                    project_value = {
                        CONFIG_KEY: found_prj.config,
                        SAMPLE_RAW_DICT_KEY: [
                            sample_sa.sample for sample_sa in found_prj.samples_mapping
                        ],
                        SUBSAMPLE_RAW_LIST_KEY: subsample_list,
                    }
                    # project_value = found_prj.project_value
                    is_private = found_prj.private
                    if raw:
                        return project_value
                    else:
                        project_obj = peppy.Project().from_dict(project_value)
                        project_obj.is_private = is_private
                        return project_obj

                else:
                    raise ProjectNotFoundError(
                        f"No project found for supplied input: '{namespace}/{name}:{tag}'. "
                        f"Did you supply a valid namespace and project?"
                    )

        except NoResultFound:
            raise ProjectNotFoundError

    @staticmethod
    def _create_select_statement(name: str, namespace: str, tag: str = DEFAULT_TAG) -> Select:
        """

        :param name:
        :param namespace:
        :param tag:
        :return:
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
        with self._sa_engine.begin() as conn:
            conn.execute(
                delete(Projects).where(
                    and_(
                        Projects.namespace == namespace,
                        Projects.name == name,
                        Projects.tag == tag,
                    )
                )
            )

        _LOGGER.info(f"Project '{namespace}/{name}:{tag} was successfully deleted'")

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
        project: peppy.Project,
        namespace: str,
        name: str = None,
        tag: str = DEFAULT_TAG,
        description: str = None,
        is_private: bool = False,
        pep_schema: str = None,
        overwrite: bool = False,
        update_only: bool = False,
    ) -> None:
        """
        Upload project to the database.
        Project with the key, that already exists won't be uploaded(but case, when argument
        update is set True)

        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param is_private: boolean value if the project should be visible just for user that creates it.
        :param pep_schema: assign PEP to a specific schema. [DefaultL: None]
        :param overwrite: if project exists overwrite the project, otherwise upload it.
            [Default: False - project won't be overwritten if it exists in db]
        :param update_only: if project exists overwrite it, otherwise do nothing.  [Default: False]
        :param description: description of the project
        :return: None
        """
        proj_dict = project.to_dict(extended=True, orient="records")
        if not description:
            description = project.description
        proj_dict[CONFIG_KEY][DESCRIPTION_KEY] = description

        namespace = namespace.lower()
        if name:
            proj_name = name.lower()
        elif proj_dict[CONFIG_KEY][NAME_KEY]:
            proj_name = proj_dict[CONFIG_KEY][NAME_KEY].lower()
        else:
            raise ValueError(f"Name of the project wasn't provided. Project will not be uploaded.")

        proj_dict[CONFIG_KEY][NAME_KEY] = proj_name

        proj_digest = create_digest(proj_dict)
        number_of_samples = len(project.samples)

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
                    pep_schema=pep_schema,
                    description=description,
                )

                self._add_samples_to_project(new_prj, proj_dict[SAMPLE_RAW_DICT_KEY])

                if proj_dict[SUBSAMPLE_RAW_LIST_KEY]:
                    subsamples = proj_dict[SUBSAMPLE_RAW_LIST_KEY]
                    self._add_subsamples_to_project(new_prj, subsamples)

                with Session(self._sa_engine) as session:
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
                        f"Namespace, name and tag already exists. Project won't be "
                        f"uploaded. Solution: Set overwrite value as True"
                        f" (project will be overwritten), or change tag!"
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
        pep_schema: str = None,
        description: str = "",
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
                    found_prj.pep_schema = pep_schema
                    found_prj.config = project_dict[CONFIG_KEY]
                    found_prj.description = description

                    # Deleting old samples and subsamples
                    if found_prj.samples_mapping:
                        for sample in found_prj.samples_mapping:
                            _LOGGER.info(f"deleting samples: {str(sample)}")
                            session.delete(sample)

                    if found_prj.subsamples_mapping:
                        for subsample in found_prj.subsamples_mapping:
                            _LOGGER.debug(f"deleting subsamples: {str(subsample)}")
                            session.delete(subsample)

                # Adding new samples and subsamples
                self._add_samples_to_project(found_prj, project_dict[SAMPLE_RAW_DICT_KEY])

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
    ) -> None:
        """
        Update partial parts of the record in db

        :param update_dict: dict with update key->values. Dict structure:
            {
                    project: Optional[peppy.Project]
                    is_private: Optional[bool]
                    tag: Optional[str]
                    name: Optional[str]
            }
        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
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
                found_prj = session.scalars(statement).one()

                if found_prj:
                    _LOGGER.debug(
                        f"Project has been found: {found_prj.namespace}, {found_prj.name}"
                    )

                    for k, v in update_values.items():
                        if getattr(found_prj, k) != v:
                            setattr(found_prj, k, v)

                            # standardizing project name
                            if k == NAME_KEY:
                                if "config" in update_values:
                                    update_values["config"][NAME_KEY] = v
                                else:
                                    found_prj.config[NAME_KEY] = v
                                found_prj.name = found_prj.config[NAME_KEY]

                    if "samples" in update_dict:
                        if found_prj.samples_mapping:
                            for sample in found_prj.samples_mapping:
                                _LOGGER.debug(f"deleting samples: {str(sample)}")
                                session.delete(sample)

                        self._add_samples_to_project(found_prj, update_dict["samples"])

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
    def __create_update_dict(update_values: UpdateItems) -> dict:
        """
        Modify keys and values that set for update and create unified
        dictionary of the values that have to be updated

         :param update_values: UpdateItems (pydantic class) with
            updating values
        :return: unified update dict
        """
        update_final = UpdateModel()

        if update_values.name is not None:
            if update_values.config is not None:
                update_values.config[NAME_KEY] = update_values.name
            update_final = UpdateModel(
                name=update_values.name,
                **update_final.dict(exclude_unset=True),
            )

        if update_values.description is not None:
            if update_values.config is not None:
                update_values.config[DESCRIPTION_KEY] = update_values.description
            update_final = UpdateModel(
                description=update_values.description,
                **update_final.dict(exclude_unset=True),
            )
        if update_values.config is not None:
            update_final = UpdateModel(
                config=update_values.config, **update_final.dict(exclude_unset=True)
            )
            name = update_values.config.get(NAME_KEY)
            description = update_values.config.get(DESCRIPTION_KEY)
            if name:
                update_final = UpdateModel(
                    name=name,
                    **update_final.dict(exclude_unset=True, exclude={NAME_KEY}),
                )
            if description:
                update_final = UpdateModel(
                    description=description,
                    **update_final.dict(exclude_unset=True, exclude={DESCRIPTION_KEY}),
                )

        if update_values.tag is not None:
            update_final = UpdateModel(
                tag=update_values.tag, **update_final.dict(exclude_unset=True)
            )

        if update_values.is_private is not None:
            update_final = UpdateModel(
                is_private=update_values.is_private,
                **update_final.dict(exclude_unset=True),
            )

        if update_values.pep_schema is not None:
            update_final = UpdateModel(
                pep_schema=update_values.pep_schema,
                **update_final.dict(exclude_unset=True),
            )

        if update_values.number_of_samples is not None:
            update_final = UpdateModel(
                number_of_samples=update_values.number_of_samples,
                **update_final.dict(exclude_unset=True),
            )

        return update_final.dict(exclude_unset=True, exclude_none=True)

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
    def _add_samples_to_project(projects_sa: Projects, samples: List[dict]) -> NoReturn:
        """
        Add samples to the project sa object. (With commit this samples will be added to the 'samples table')
        :param projects_sa: Projects sa object, in open session
        :param samples: list of samles to be added to the database
        :return: NoReturn
        """
        for row_number, sample in enumerate(samples):
            projects_sa.samples_mapping.append(Samples(sample=sample, row_number=row_number))

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
