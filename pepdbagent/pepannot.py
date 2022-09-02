from .const import (
    STATUS_KEY,
    DESCRIPTION_KEY,
    N_SAMPLES_KEY,
    UPDATE_DATE_KEY,
    BASE_ANNOTATION_DICT,
    DEFAULT_STATUS,
)
import json
import logmuse
import coloredlogs

_LOGGER = logmuse.init_logger("pepannot")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


class Annotation(dict):
    """
    A class to model an annotations used in pep-db
    """

    def __init__(self, annotation_dict: dict = None, registry: str = None):

        super(Annotation, self).__init__()
        if annotation_dict is None:
            annotation_dict = BASE_ANNOTATION_DICT
        self.registry = registry

        self.annotation_dict = annotation_dict

        for dict_key in annotation_dict.keys():
            self[dict_key] = annotation_dict[dict_key]

        for dict_key in BASE_ANNOTATION_DICT:
            if dict_key not in self:
                self[dict_key] = BASE_ANNOTATION_DICT[dict_key]

        self._status = None
        self._description = None
        self._n_samples = None
        self._last_update = None
        self._property_setter(annotation_dict)

    @classmethod
    def init_empty_annotation(cls):
        """
        Initiate empty annotation
        :return: Annotation
        """
        init_dict = BASE_ANNOTATION_DICT
        return Annotation(annotation_dict=init_dict)

    @classmethod
    def create_new_annotation(
        cls,
        status: str = None,
        last_update: str = None,
        n_samples: int = None,
        description: str = None,
        anno_dict: dict = None,
    ):
        """
        Create a new annotation for pep-db
        :param status: pep status
        :param last_update: pep last update
        :param n_samples: number of samples in pep
        :param description: description of PEP
        :param anno_dict: other
        :return: Annotation class
        """
        new_dict = BASE_ANNOTATION_DICT
        if status:
            new_dict[STATUS_KEY] = status
        else:
            new_dict[STATUS_KEY] = DEFAULT_STATUS
        if last_update:
            new_dict[UPDATE_DATE_KEY] = last_update
        if n_samples:
            new_dict[N_SAMPLES_KEY] = n_samples
        if description:
            new_dict[DESCRIPTION_KEY] = description
        if anno_dict:
            try:
                if not isinstance(anno_dict, dict):
                    assert TypeError
                for dict_key in anno_dict.keys():
                    new_dict[dict_key] = anno_dict[dict_key]
            except TypeError:
                _LOGGER.error(
                    "You have provided incorrect annotation dictionary type. "
                    "It's not a dict"
                )
            except AttributeError:
                _LOGGER.error("Incorrect annotation dictionary type. Continuing..")
        return Annotation(annotation_dict=new_dict)

    def _property_setter(self, annot_dict: dict):
        """
        Initialization of setters from annot_dict
        :param annot_dict: Annotation dict
        """
        if STATUS_KEY in annot_dict:
            self.status = annot_dict[STATUS_KEY]
        else:
            self.status = "Unknown"

        if DESCRIPTION_KEY in annot_dict:
            self.description = annot_dict[DESCRIPTION_KEY]
        else:
            self.description = ""

        if N_SAMPLES_KEY in annot_dict:
            self.n_samples = annot_dict[N_SAMPLES_KEY]
        else:
            self.n_samples = None

        if UPDATE_DATE_KEY in annot_dict:
            self.last_update = annot_dict[UPDATE_DATE_KEY]
        else:
            self.last_update = None

    def get_json(self):
        return json.dumps(dict(self))

    @property
    def status(self) -> str:
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        self[STATUS_KEY] = value

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str):
        self._description = description
        self[DESCRIPTION_KEY] = description

    @property
    def n_samples(self) -> str:
        return self._n_samples

    @n_samples.setter
    def n_samples(self, n_samples: str):
        self._n_samples = n_samples
        self[N_SAMPLES_KEY] = n_samples

    @property
    def last_update(self) -> str:
        return self._last_update

    @last_update.setter
    def last_update(self, last_update: str):
        self._last_update = last_update
        self[UPDATE_DATE_KEY] = last_update

    @property
    def registry(self) -> str:
        return self.__registry

    @registry.setter
    def registry(self, registry: str):
        self.__registry = registry

    def __str__(self):
        return f"This is annotation of the project: '{self.registry}'. \nAnnotations: \n{dict(self)}"

    def __dict__(self):
        return dict(self)
