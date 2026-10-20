# Sample index: what we work out about each sample, kept apart from what the submitter wrote.
# See ADR pephub/index_tables_adr.md.

import datetime
import hashlib
import json
import logging
from collections.abc import Iterable

from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from pepdbagent.const import INDEX_CONFIDENCE_LEVELS, INDEX_FIELDS, PKG_NAME
from pepdbagent.db_utils import (
    BaseEngine,
    IndexClaim,
    IndexIdentifier,
    IndexMapping,
    IndexPepLocation,
    IndexRelation,
    IndexRun,
    IndexSubject,
    IndexTerm,
    IndexTermAncestor,
    Projects,
)
from pepdbagent.models import (
    IndexClaimIn,
    IndexClaimOut,
    IndexIdentifierIn,
    IndexLoadResult,
    IndexPepLocationIn,
    IndexRelationIn,
    IndexRunIn,
    IndexSubjectRef,
)

_LOGGER = logging.getLogger(PKG_NAME)

CHUNK = 5000


def _chunks(items: list, size: int = CHUNK):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _mapping_key(mapping: dict) -> str:
    return hashlib.sha256(json.dumps(mapping, sort_keys=True).encode()).hexdigest()[:32]


class PEPDatabaseIndex:
    """
    Class that represents the sample index in the database.

    Labelers write claims through runs: loading a run replaces the claims of earlier
    runs of the same labeler over the same scope, and leaves everything else alone.
    Each distinct mapping (submitter text -> term, by a rule) is stored once in
    idx.mapping; a claim is just (subject, mapping, run).
    """

    def __init__(self, pep_db_engine: BaseEngine):
        """
        Args:
            pep_db_engine: PEPDatabaseAgent engine object.
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def replace_run(
        self,
        run: IndexRunIn,
        claims: Iterable[IndexClaimIn],
        identifiers: Iterable[IndexIdentifierIn] = (),
        relations: Iterable[IndexRelationIn] = (),
        pep_locations: Iterable[IndexPepLocationIn] = (),
    ) -> IndexLoadResult:
        """Load one labeler run in a single transaction.

        Subjects, identifiers, relations, and PEP locations are added if new. Claims
        from earlier runs with the same labeler and scope are deleted; claims from
        other labelers or scopes are kept.

        Args:
            run: Which labeler produced the claims, over what scope, with which versions.
            claims: Claims to load.
            identifiers: Other identifiers of subjects (SRA, BioSample, ...).
            relations: Links between subjects (study has_sample sample, ...).
            pep_locations: Projects holding the subjects' raw rows.

        Returns:
            Counts of what was loaded.

        Raises:
            ValueError: If a claim has an unknown field or confidence level.
        """
        claims, identifiers = list(claims), list(identifiers)
        relations, pep_locations = list(relations), list(pep_locations)
        for c in claims:
            if c.field not in INDEX_FIELDS:
                raise ValueError(
                    f"Unknown index field '{c.field}' ({c.subject.accession})"
                )
            if c.confidence_level not in INDEX_CONFIDENCE_LEVELS:
                raise ValueError(
                    f"Unknown confidence level '{c.confidence_level}' ({c.subject.accession})"
                )

        refs = {(c.subject.kind, c.subject.accession) for c in claims}
        refs |= {(i.subject.kind, i.subject.accession) for i in identifiers}
        refs |= {(r.from_subject.kind, r.from_subject.accession) for r in relations}
        refs |= {(r.to_subject.kind, r.to_subject.accession) for r in relations}
        refs |= {(p.subject.kind, p.subject.accession) for p in pep_locations}

        with Session(self._sa_engine) as session, session.begin():
            run_row = IndexRun(**run.model_dump())
            session.add(run_row)
            session.flush()

            ids = self._upsert_subjects(session, sorted(refs))

            def sid(ref: IndexSubjectRef) -> int:
                return ids[(ref.kind, ref.accession)]

            for chunk in _chunks(
                [
                    {
                        "subject_id": sid(i.subject),
                        "scheme": i.scheme,
                        "value": i.value,
                        "source": i.source,
                    }
                    for i in identifiers
                ]
            ):
                session.execute(insert(IndexIdentifier).on_conflict_do_nothing(), chunk)

            for chunk in _chunks(
                [
                    {
                        "from_id": sid(r.from_subject),
                        "rel": r.rel,
                        "to_id": sid(r.to_subject),
                    }
                    for r in relations
                ]
            ):
                session.execute(insert(IndexRelation).on_conflict_do_nothing(), chunk)

            located, missing = self._add_pep_locations(session, pep_locations, sid)

            old_runs = select(IndexRun.id).where(
                IndexRun.labeler == run.labeler,
                IndexRun.scope == run.scope,
                IndexRun.id != run_row.id,
            )
            replaced = session.execute(
                delete(IndexClaim).where(IndexClaim.run_id.in_(old_runs))
            ).rowcount

            mappings = [c.model_dump(exclude={"subject"}) for c in claims]
            mapping_ids = self._upsert_mappings(session, mappings)
            rows = {
                (sid(c.subject), mapping_ids[_mapping_key(m)])
                for c, m in zip(claims, mappings)
            }
            for chunk in _chunks(
                [
                    {"subject_id": s, "mapping_id": m, "run_id": run_row.id}
                    for s, m in sorted(rows)
                ]
            ):
                session.execute(insert(IndexClaim), chunk)

            session.execute(
                delete(IndexMapping).where(
                    ~select(IndexClaim.mapping_id)
                    .where(IndexClaim.mapping_id == IndexMapping.id)
                    .exists()
                )
            )

            run_row.finished = datetime.datetime.now(datetime.timezone.utc)
            run_id = run_row.id

        _LOGGER.info(
            f"Index run {run_id} ({run.labeler}, {run.scope}): {len(rows)} claims, {replaced} replaced"
        )
        return IndexLoadResult(
            run_id=run_id,
            subjects=len(refs),
            claims=len(rows),
            claims_replaced=replaced,
            identifiers=len(identifiers),
            relations=len(relations),
            pep_locations=located,
            pep_locations_missing=missing,
        )

    def _upsert_subjects(self, session: Session, refs: list[tuple[str, str]]) -> dict:
        ids = {}
        for chunk in _chunks(refs):
            session.execute(
                insert(IndexSubject).on_conflict_do_nothing(),
                [{"kind": k, "accession": a} for k, a in chunk],
            )
            rows = session.execute(
                select(
                    IndexSubject.kind, IndexSubject.accession, IndexSubject.id
                ).where(tuple_(IndexSubject.kind, IndexSubject.accession).in_(chunk))
            )
            ids.update({(k, a): i for k, a, i in rows})
        return ids

    def _upsert_mappings(self, session: Session, mappings: list[dict]) -> dict:
        unique = {_mapping_key(m): m for m in mappings}
        ids = {}
        for chunk in _chunks(sorted(unique)):
            session.execute(
                insert(IndexMapping).on_conflict_do_nothing(),
                [{"key": k, **unique[k]} for k in chunk],
            )
            rows = session.execute(
                select(IndexMapping.key, IndexMapping.id).where(
                    IndexMapping.key.in_(chunk)
                )
            )
            ids.update(dict(rows.all()))
        return ids

    def _add_pep_locations(
        self, session: Session, pep_locations: list, sid
    ) -> tuple[int, int]:
        keys = sorted({(p.namespace, p.name, p.tag) for p in pep_locations})
        project_ids = {}
        for chunk in _chunks(keys):
            rows = session.execute(
                select(
                    Projects.namespace, Projects.name, Projects.tag, Projects.id
                ).where(
                    tuple_(Projects.namespace, Projects.name, Projects.tag).in_(chunk)
                )
            )
            project_ids.update({(n, m, t): i for n, m, t, i in rows})

        rows = [
            {
                "subject_id": sid(p.subject),
                "project_id": project_ids[(p.namespace, p.name, p.tag)],
                "match_key": p.match_key,
            }
            for p in pep_locations
            if (p.namespace, p.name, p.tag) in project_ids
        ]
        for chunk in _chunks(rows):
            stmt = insert(IndexPepLocation)
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["subject_id", "project_id"],
                    set_={"match_key": stmt.excluded.match_key},
                ),
                chunk,
            )
        return len(rows), len(pep_locations) - len(rows)

    def get_claims(self, accession: str, kind: str = "sample") -> list[IndexClaimOut]:
        """Get all claims about one subject.

        Args:
            accession: Public accession, e.g. GSM9903795.
            kind: 'sample' or 'study'.

        Returns:
            Claims, ordered by field.
        """
        statement = (
            select(IndexClaim.run_id, IndexMapping, IndexRun)
            .join(IndexMapping, IndexMapping.id == IndexClaim.mapping_id)
            .join(IndexRun, IndexRun.id == IndexClaim.run_id)
            .join(IndexSubject, IndexSubject.id == IndexClaim.subject_id)
            .where(IndexSubject.kind == kind, IndexSubject.accession == accession)
            .order_by(IndexMapping.field, IndexClaim.run_id, IndexMapping.id)
        )
        with Session(self._sa_engine) as session:
            return [
                IndexClaimOut(
                    subject=IndexSubjectRef(kind=kind, accession=accession),
                    run_id=run_id,
                    mapping_tool=r.mapping_tool,
                    mapping_tool_version=r.mapping_tool_version,
                    **{
                        k: getattr(m, k)
                        for k in IndexClaimIn.model_fields
                        if k != "subject"
                    },
                )
                for run_id, m, r in session.execute(statement)
            ]

    def replace_terms(
        self,
        terms: Iterable[dict],
        ancestors: Iterable[tuple[str, str]],
        ontologies: Iterable[str],
    ) -> int:
        """Replace the stored terms and closure rows for the given ontologies.

        Args:
            terms: Dicts with term_id, label, ontology, version.
            ancestors: (term_id, ancestor_id) pairs, already closed over is_a and part_of.
            ontologies: Ontologies being replaced, e.g. ['UBERON', 'CL'].

        Returns:
            Number of terms stored.
        """
        terms, ancestors, ontologies = list(terms), list(ancestors), list(ontologies)
        with Session(self._sa_engine) as session, session.begin():
            old = select(IndexTerm.term_id).where(IndexTerm.ontology.in_(ontologies))
            session.execute(
                delete(IndexTermAncestor).where(IndexTermAncestor.term_id.in_(old))
            )
            session.execute(delete(IndexTerm).where(IndexTerm.ontology.in_(ontologies)))
            for chunk in _chunks(terms):
                session.execute(insert(IndexTerm).on_conflict_do_nothing(), chunk)
            for chunk in _chunks(
                [{"term_id": t, "ancestor_id": a} for t, a in ancestors], 20000
            ):
                session.execute(
                    insert(IndexTermAncestor).on_conflict_do_nothing(), chunk
                )
        return len(terms)

    def count_claims(self) -> int:
        with Session(self._sa_engine) as session:
            return session.scalar(select(func.count()).select_from(IndexClaim))
