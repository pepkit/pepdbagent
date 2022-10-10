DB_TABLE_NAME = "projects"
ID_COL = "id"
PROJ_COL = "project_value"
ANNO_COL = "anno_info"
NAMESPACE_COL = "namespace"
NAME_COL = "name"
TAG_COL = "tag"
DIGEST_COL = "digest"

DB_COLUMNS = [ID_COL, DIGEST_COL, PROJ_COL, ANNO_COL, NAMESPACE_COL, NAME_COL, TAG_COL]

DEFAULT_NAMESPACE = "_"
DEFAULT_TAG = "default"

STATUS_KEY = "status"
DESCRIPTION_KEY = "description"
N_SAMPLES_KEY = "n_samples"
UPDATE_DATE_KEY = "last_update"
IS_PRIVATE_KEY = "is_private"
DEFAULT_STATUS = "Unknown"

BASE_ANNOTATION_DICT = {
    STATUS_KEY: DEFAULT_STATUS,
    DESCRIPTION_KEY: None,
    N_SAMPLES_KEY: None,
    UPDATE_DATE_KEY: None,
}

from peppy.const import SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_DICT_KEY
