DB_TABLE_NAME = "projects"
ID_COL = "id"
PROJ_COL = "project_value"
ANNO_COL = "anno_info"
NAMESPACE_COL = "namespace"
NAME_COL = "name"
TAG_COL = "tag"
DIGEST_COL = "digest"
PRIVATE_COL = "private"

DB_COLUMNS = [ID_COL, DIGEST_COL, PROJ_COL, ANNO_COL, NAMESPACE_COL, NAME_COL, TAG_COL, PRIVATE_COL]

DEFAULT_NAMESPACE = "_"
DEFAULT_TAG = "default"

DESCRIPTION_KEY = "description"
N_SAMPLES_KEY = "number_of_samples"
UPDATE_DATE_KEY = "last_update"
IS_PRIVATE_KEY = "is_private"

from peppy.const import SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_DICT_KEY

DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 100
