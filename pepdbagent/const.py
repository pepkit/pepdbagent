DB_TABLE_NAME = "projects"
ID_COL = "id"
PROJ_COL = "project_value"
ANNO_COL = "anno_info"
NAMESPACE_COL = "namespace"
NAME_COL = "name"
TAG_COL = "tag"
DIGEST_COL = "digest"
PRIVATE_COL = "private"
N_SAMPLES_COL = "number_of_samples"
SUBMISSION_DATE_COL = "submission_date"
LAST_UPDATE_DATE_COL = "last_update_date"


DB_COLUMNS = [
    ID_COL,
    DIGEST_COL,
    PROJ_COL,
    NAMESPACE_COL,
    NAME_COL,
    TAG_COL,
    PRIVATE_COL,
    N_SAMPLES_COL,
    SUBMISSION_DATE_COL,
    LAST_UPDATE_DATE_COL,
]

DEFAULT_NAMESPACE = "_"
DEFAULT_TAG = "default"

DESCRIPTION_KEY = "description"

from peppy.const import SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_DICT_KEY

DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 100
