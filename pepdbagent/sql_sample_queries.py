""" This script holds the SQL queries for the PEPDBAgent. """

from sqlalchemy import text, TextClause


def get_samples_query(project_id: int) -> TextClause:
    """
    Get sql text (TextClause) for getting samples from the database. In correct order.

    :param project_id: int: project id

    :return: TextClause: sql text for getting samples from the database
    """
    get_samples_text = text(
        """
        WITH RECURSIVE sample_hierarchy AS (
                            -- Anchor member: Select the root sample(s)
                            SELECT
                                s.id,
                                s.sample,
                                s.row_number,
                                s.project_id,
                                s.sample_name,
                                s.guid,
                                s.parent_guid,
                                0 AS depth
                            FROM
                                samples s
                            WHERE
                                s.project_id = :prj_id
                                AND s.parent_guid IS NULL  -- Root samples
    
                            UNION ALL
    
                            -- Recursive member: Select child samples
                            SELECT
                                s.id,
                                s.sample,
                                s.row_number,
                                s.project_id,
                                s.sample_name,
                                s.guid,
                                s.parent_guid,
                                sh.depth + 1 AS depth
                            FROM
                                samples s
                            JOIN
                                sample_hierarchy sh ON s.parent_guid = sh.guid
                        )
                        SELECT *
                        FROM sample_hierarchy
                            order by depth;
                            """
    ).bindparams(prj_id=project_id)
    return get_samples_text


def get_last_sample_id(project_id: int) -> TextClause:
    get_last_sample_id_text = text(
        """
        WITH RECURSIVE sample_hierarchy AS (
            -- Anchor member: Select the root sample(s)
            SELECT
                s.guid,
                s.parent_guid,
                0 AS depth
            FROM
                samples s
            WHERE
                s.project_id = :prj_id
                AND s.parent_guid IS NULL  -- Root samples
    
            UNION ALL
    
            -- Recursive member: Select child samples
            SELECT
                s.guid,
                s.parent_guid,
                sh.depth + 1 AS depth
            FROM
                samples s
            JOIN
                sample_hierarchy sh ON s.parent_guid = sh.guid
        )
        SELECT guid
        FROM sample_hierarchy
            order by depth desc
                limit 1;
        """
    ).bindparams(prj_id=project_id)
    return get_last_sample_id_text
