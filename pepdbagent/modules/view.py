# View of the PEP. In other words, it is a part of the PEP, or subset of the samples in the PEP.

import logging

import peprs
from sqlalchemy import and_, delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pepdbagent.const import DEFAULT_TAG, PKG_NAME
from pepdbagent.db_utils import (
    BaseEngine,
    Projects,
    Samples,
    Views,
    ViewSampleAssociation,
)
from pepdbagent.exceptions import (
    ProjectNotFoundError,
    SampleAlreadyInView,
    SampleNotFoundError,
    SampleNotInViewError,
    ViewAlreadyExistsError,
    ViewNotFoundError,
)
from pepdbagent.models import CreateViewDictModel, ProjectViews, ViewAnnotation

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseView:
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
        tag: str = DEFAULT_TAG,
        view_name: str = None,
        raw: bool = True,
    ) -> peprs.Project | dict | None:
        """Retrieve a view of the project from the database.

        A view is a subset of samples in the project.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            view_name: Name of the view.
            raw: Return raw dict if True, peprs.Project object if False.

        Returns:
            Raw dict or peprs.Project with only the view's samples.

        Raises:
            ViewNotFoundError: If the view does not exist.
        """
        _LOGGER.debug(f"Get view {view_name} from {namespace}/{name}:{tag}")
        view_statement = select(Views).where(
            and_(
                Views.project_mapping.has(namespace=namespace, name=name, tag=tag),
                Views.name == view_name,
            )
        )

        with Session(self._sa_engine) as sa_session:
            view = sa_session.scalar(view_statement)
            if not view:
                raise ViewNotFoundError(
                    f"View {view_name} of the project {namespace}/{name}:{tag} does not exist"
                )
            samples = [sample.sample.sample for sample in view.samples]
            config = view.project_mapping.config
        sub_project_dict = {"config": config, "samples": samples}
        if raw:
            return sub_project_dict
        else:
            return peprs.Project.from_dict(sub_project_dict)

    def get_annotation(
        self, namespace: str, name: str, tag: str = DEFAULT_TAG, view_name: str = None
    ) -> ViewAnnotation:
        """Get annotation for a view.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            view_name: Name of the view.

        Returns:
            ViewAnnotation with project coordinates, name, description, and sample count.

        Raises:
            ViewNotFoundError: If the view does not exist.
        """
        _LOGGER.debug(
            f"Get annotation for view {view_name} in {namespace}/{name}:{tag}"
        )
        view_statement = select(Views).where(
            and_(
                Views.project_mapping.has(namespace=namespace, name=name, tag=tag),
                Views.name == view_name,
            )
        )

        with Session(self._sa_engine) as sa_session:
            view = sa_session.scalar(view_statement)
            if not view:
                raise ViewNotFoundError(
                    f"View {name} of the project {namespace}/{name}:{tag} does not exist"
                )
            return ViewAnnotation(
                project_namespace=namespace,
                project_name=name,
                project_tag=tag,
                name=view.name,
                description=view.description,
                number_of_samples=len(view.samples),
            )

    def create(
        self,
        view_name: str,
        view_dict: dict | CreateViewDictModel,
        description: str = None,
        no_fail: bool = False,
    ) -> None:
        """Create a view of a project in the database.

        Args:
            view_name: Name for the new view.
            view_dict: View definition with project_namespace, project_name, project_tag,
                and sample_list keys (or a CreateViewDictModel).
            description: Optional description of the view.
            no_fail: Skip samples that do not exist instead of raising an error.

        Raises:
            ProjectNotFoundError: If the project does not exist.
            SampleNotFoundError: If a sample does not exist and no_fail is False.
            ViewAlreadyExistsError: If a view with the same name already exists.
        """
        _LOGGER.debug(
            f"Creating view {view_name} with provided info: (view_dict: {view_dict})"
        )
        if isinstance(view_dict, dict):
            view_dict = CreateViewDictModel(**view_dict)

        project_statement = select(Projects).where(
            and_(
                Projects.namespace == view_dict.project_namespace,
                Projects.name == view_dict.project_name,
                Projects.tag == view_dict.project_tag,
            )
        )
        try:
            with Session(self._sa_engine) as sa_session:
                project = sa_session.scalar(project_statement)
                if not project:
                    raise ProjectNotFoundError(
                        f"Project {view_dict.project_namespace}/{view_dict.project_name}:{view_dict.project_tag} does not exist"
                    )
                view = Views(
                    name=view_name,
                    description=description,
                    project_mapping=project,
                )
                sa_session.add(view)

                for sample_name in view_dict.sample_list:
                    sample_statement = select(Samples.id).where(
                        and_(
                            Samples.project_id == project.id,
                            Samples.sample_name == sample_name,
                        )
                    )
                    sample_id_tuple = sa_session.execute(sample_statement).one_or_none()
                    if sample_id_tuple:
                        sample_id = sample_id_tuple[0]
                    elif not sample_id_tuple and not no_fail:
                        raise SampleNotFoundError(
                            f"Sample {view_dict.project_namespace}/{view_dict.project_name}:{view_dict.project_tag}:{sample_name} does not exist"
                        )
                    else:
                        continue

                    sa_session.add(
                        ViewSampleAssociation(sample_id=sample_id, view=view)
                    )

                sa_session.commit()
        except IntegrityError:
            raise ViewAlreadyExistsError(
                f"View {view_name} of the project {view_dict.project_namespace}/{view_dict.project_name}:{view_dict.project_tag} already exists"
            )

    def delete(
        self,
        project_namespace: str,
        project_name: str,
        project_tag: str = DEFAULT_TAG,
        view_name: str = None,
    ) -> None:
        """Delete a view from the database.

        Args:
            project_namespace: Namespace of the project.
            project_name: Name of the project.
            project_tag: Tag of the project.
            view_name: Name of the view to delete.

        Raises:
            ViewNotFoundError: If the view does not exist.
        """
        _LOGGER.debug(
            f"Deleting view {view_name} from {project_namespace}/{project_name}:{project_tag}"
        )
        view_statement = select(Views).where(
            and_(
                Views.project_mapping.has(
                    namespace=project_namespace, name=project_name, tag=project_tag
                ),
                Views.name == view_name,
            )
        )

        with Session(self._sa_engine) as sa_session:
            view = sa_session.scalar(view_statement)
            if not view:
                raise ViewNotFoundError(
                    f"View {view_name} of the project {project_namespace}/{project_name}:{project_tag} does not exist"
                )
            sa_session.delete(view)
            sa_session.commit()

    def add_sample(
        self,
        namespace: str,
        name: str,
        tag: str,
        view_name: str,
        sample_name: str | list[str],
    ) -> None:
        """Add one or more samples to a view.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            view_name: Name of the view.
            sample_name: Sample name or list of sample names.

        Raises:
            ViewNotFoundError: If the view does not exist.
            SampleNotFoundError: If a sample does not exist.
            SampleAlreadyInView: If a sample is already in the view.
        """
        _LOGGER.debug(
            f"Adding sample {sample_name} to view {view_name} in {namespace}/{name}:{tag}"
        )
        if isinstance(sample_name, str):
            sample_name = [sample_name]
        view_statement = select(Views).where(
            and_(
                Views.project_mapping.has(namespace=namespace, name=name, tag=tag),
                Views.name == view_name,
            )
        )
        try:
            with Session(self._sa_engine) as sa_session:
                view = sa_session.scalar(view_statement)
                if not view:
                    raise ViewNotFoundError(
                        f"View {view_name} of the project {namespace}/{name}:{tag} does not exist"
                    )
                for sample_name_one in sample_name:
                    sample_statement = select(Samples).where(
                        and_(
                            Samples.project_id == view.project_mapping.id,
                            Samples.sample_name == sample_name_one,
                        )
                    )
                    sample = sa_session.scalar(sample_statement)
                    if not sample:
                        raise SampleNotFoundError(
                            f"Sample {namespace}/{name}:{tag}:{sample_name} does not exist"
                        )

                    sa_session.add(ViewSampleAssociation(sample=sample, view=view))
                    sa_session.commit()
        except IntegrityError:
            raise SampleAlreadyInView(
                f"Sample {namespace}/{name}:{tag}:{sample_name} already in view {view_name}"
            )

    def remove_sample(
        self,
        namespace: str,
        name: str,
        tag: str,
        view_name: str,
        sample_name: str,
    ) -> None:
        """Remove a sample from a view.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            view_name: Name of the view.
            sample_name: Name of the sample to remove.

        Raises:
            ViewNotFoundError: If the view does not exist.
            SampleNotInViewError: If the sample is not in the view.
        """
        _LOGGER.debug(
            f"Removing sample {sample_name} from view {view_name} in {namespace}/{name}:{tag}"
        )
        view_statement = select(Views).where(
            and_(
                Views.project_mapping.has(namespace=namespace, name=name, tag=tag),
                Views.name == view_name,
            )
        )

        with Session(self._sa_engine) as sa_session:
            view = sa_session.scalar(view_statement)
            if not view:
                raise ViewNotFoundError(
                    f"View {view_name} of the project {namespace}/{name}:{tag} does not exist"
                )
            sample_statement = select(Samples).where(
                and_(
                    Samples.project_id == view.project_mapping.id,
                    Samples.sample_name == sample_name,
                )
            )
            sample = sa_session.scalar(sample_statement)
            if sample.id not in [view_sample.sample_id for view_sample in view.samples]:
                raise SampleNotInViewError(
                    f"Sample {namespace}/{name}:{tag}:{sample_name} does not exist in view {view_name}"
                )
            delete_statement = delete(ViewSampleAssociation).where(
                and_(
                    ViewSampleAssociation.sample_id == sample.id,
                    ViewSampleAssociation.view_id == view.id,
                )
            )
            sa_session.execute(delete_statement)
            sa_session.commit()

    def get_snap_view(
        self,
        namespace: str,
        name: str,
        tag: str,
        sample_name_list: list[str],
        raw: bool = False,
    ) -> peprs.Project | dict:
        """Get an ephemeral view of a project limited to a set of samples.

        The snap view is not persisted in the database.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.
            sample_name_list: Sample names to include, e.g., ["sample1", "sample2"].
            raw: Return raw dict if True, peprs.Project object if False.

        Returns:
            Raw dict or peprs.Project containing only the requested samples.

        Raises:
            ProjectNotFoundError: If the project does not exist.
            SampleNotFoundError: If any sample does not exist.
        """
        _LOGGER.debug(f"Creating snap view for {namespace}/{name}:{tag}")
        project_statement = select(Projects).where(
            and_(
                Projects.namespace == namespace,
                Projects.name == name,
                Projects.tag == tag,
            )
        )
        with Session(self._sa_engine) as sa_session:
            project = sa_session.scalar(project_statement)
            if not project:
                raise ProjectNotFoundError(
                    f"Project {namespace}/{name}:{tag} does not exist"
                )
            samples = []
            for sample_name in sample_name_list:
                sample_statement = select(Samples).where(
                    and_(
                        Samples.project_id == project.id,
                        Samples.sample_name == sample_name,
                    )
                )
                sample = sa_session.scalar(sample_statement)
                if not sample:
                    raise SampleNotFoundError(
                        f"Sample {namespace}/{name}:{tag}:{sample_name} does not exist"
                    )
                samples.append(sample.sample)
            config = project.config

        if raw:
            return {"config": config, "samples": samples}
        else:
            return peprs.Project.from_dict({"config": config, "samples": samples})

    def get_views_annotation(
        self, namespace: str, name: str, tag: str = DEFAULT_TAG
    ) -> ProjectViews | None:
        """Get annotation for all views of a project.

        Args:
            namespace: Namespace of the project.
            name: Name of the project.
            tag: Tag of the project.

        Returns:
            ProjectViews with a list of ViewAnnotation objects.
        """
        _LOGGER.debug(f"Get views annotation for {namespace}/{name}:{tag}")
        statement = select(Views).where(
            Views.project_mapping.has(namespace=namespace, name=name, tag=tag),
        )
        views_list = []

        with Session(self._sa_engine) as session:
            views = session.scalars(statement)
            for view in views:
                views_list.append(
                    ViewAnnotation(
                        name=view.name,
                        description=view.description,
                        number_of_samples=len(view.samples),
                    )
                )

        return ProjectViews(namespace=namespace, name=name, tag=tag, views=views_list)
