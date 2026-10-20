"""Sample index (schema idx)

Revision ID: b7d2e4a91c03
Revises: 8a037f13b4e5
Create Date: 2026-10-19 21:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b7d2e4a91c03"
down_revision: Union[str, None] = "8a037f13b4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "idx"
FIELDS = (
    "field IN ('organism', 'assay', 'tissue', 'cell_type', 'cell_line', 'sample_type')"
)
LEVELS = "confidence_level IN ('high', 'medium', 'low')"


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    op.create_table(
        "subject",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("accession", sa.String(), nullable=False),
        sa.UniqueConstraint("kind", "accession"),
        sa.CheckConstraint("kind IN ('sample', 'study')"),
        schema=SCHEMA,
    )
    op.create_table(
        "identifier",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "subject_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{SCHEMA}.subject.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("scheme", sa.String(), nullable=False),
        sa.Column("value", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=True),
        sa.UniqueConstraint("scheme", "value", "subject_id"),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_identifier_value", "identifier", ["scheme", "value"], schema=SCHEMA
    )
    op.create_table(
        "relation",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "from_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{SCHEMA}.subject.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rel", sa.String(), nullable=False),
        sa.Column(
            "to_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{SCHEMA}.subject.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.UniqueConstraint("from_id", "rel", "to_id"),
        schema=SCHEMA,
    )
    op.create_index("idx_relation_to", "relation", ["to_id"], schema=SCHEMA)
    op.create_table(
        "pep_location",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "subject_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{SCHEMA}.subject.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "project_id",
            sa.Integer(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("match_key", sa.String(), nullable=False),
        sa.UniqueConstraint("subject_id", "project_id"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_idx_pep_location_project_id", "pep_location", ["project_id"], schema=SCHEMA
    )
    op.create_table(
        "run",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("labeler", sa.String(), nullable=False),
        sa.Column("code_version", sa.String(), nullable=True),
        sa.Column("maps_version", sa.String(), nullable=True),
        sa.Column("mapping_tool", sa.String(), nullable=True),
        sa.Column("mapping_tool_version", sa.String(), nullable=True),
        sa.Column("sources", postgresql.JSONB(), nullable=True),
        sa.Column("scope", sa.String(), nullable=False),
        sa.Column("started", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished", sa.TIMESTAMP(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_table(
        "mapping",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("key", sa.String(), nullable=False, unique=True),
        sa.Column("field", sa.String(), nullable=False),
        sa.Column("subject_label", sa.String(), nullable=True),
        sa.Column("subject_source", sa.String(), nullable=True),
        sa.Column("object_id", sa.String(), nullable=True),
        sa.Column("object_label", sa.String(), nullable=True),
        sa.Column("object_candidates", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column("mapping_justification", sa.String(), nullable=False),
        sa.Column("curation_rule", sa.String(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("confidence_level", sa.String(), nullable=False),
        sa.CheckConstraint(FIELDS),
        sa.CheckConstraint(LEVELS),
        sa.CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)"
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_mapping_field_object", "mapping", ["field", "object_id"], schema=SCHEMA
    )
    op.create_table(
        "claim",
        sa.Column(
            "subject_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{SCHEMA}.subject.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "mapping_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{SCHEMA}.mapping.id"),
            primary_key=True,
        ),
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey(f"{SCHEMA}.run.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_claim_mapping", "claim", ["mapping_id", "subject_id"], schema=SCHEMA
    )
    op.create_index("idx_claim_run", "claim", ["run_id"], schema=SCHEMA)
    op.create_table(
        "term",
        sa.Column("term_id", sa.String(), primary_key=True),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("ontology", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_term_label", "term", [sa.text("lower(label)")], schema=SCHEMA
    )
    op.create_table(
        "term_ancestor",
        sa.Column("term_id", sa.String(), primary_key=True),
        sa.Column("ancestor_id", sa.String(), primary_key=True),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_term_ancestor_ancestor", "term_ancestor", ["ancestor_id"], schema=SCHEMA
    )


def downgrade() -> None:
    """Downgrade schema."""
    for table in (
        "term_ancestor",
        "term",
        "claim",
        "mapping",
        "run",
        "pep_location",
        "relation",
        "identifier",
        "subject",
    ):
        op.drop_table(table, schema=SCHEMA)
    op.execute(f"DROP SCHEMA IF EXISTS {SCHEMA}")
