# Sample index: what we work out about each sample, kept apart from what the submitter wrote.
# See ADR pephub/index_tables_adr.md.

import datetime
import hashlib
import json
import logging
from collections.abc import Iterable

from sqlalchemy import (
    Integer,
    case,
    cast,
    delete,
    func,
    or_,
    select,
    text,
    tuple_,
    union_all,
)
from sqlalchemy.dialects.postgresql import ARRAY, insert
from sqlalchemy.orm import Session, aliased

from pepdbagent.const import (
    INDEX_CONFIDENCE_LEVELS,
    INDEX_FIELDS,
    INDEX_MATCH_MODES,
    INDEX_SCHEMA,
    PKG_NAME,
)
from pepdbagent.db_utils import (
    BaseEngine,
    IndexClaim,
    IndexFacetKey,
    IndexIdentifier,
    IndexMapping,
    IndexPepLocation,
    IndexRelation,
    IndexRun,
    IndexSampleFacet,
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
    IndexMatchedClaim,
    IndexPepLocationIn,
    IndexRelationIn,
    IndexRunIn,
    IndexSearchHit,
    IndexSearchResult,
    IndexSubjectRef,
)

_LOGGER = logging.getLogger(PKG_NAME)

CHUNK = 5000

# Search parameter -> claim fields it looks in. Cell lines carry a tissue claim
# from Cellosaurus, so a tissue search finds them too.
SEARCH_FIELDS = {
    "organism": ("organism",),
    "assay": ("assay",),
    "tissue": ("tissue", "cell_type"),
    "sample_type": ("sample_type",),
}
_RANK = {"low": 1, "medium": 2, "high": 3}
SIZE_PROBE = (
    100_000  # claims counted per criterion to pick where a search starts (match='best')
)
SMALL_RESULT = 20_000  # up to this many hits, collect them all and sort; above, walk samples in order


def _rank(column):
    return case(_RANK, value=column)


def _chunks(items: list, size: int = CHUNK):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _copy(session: Session, target: str, rows: Iterable[tuple]) -> None:
    """Bulk-load rows with COPY, inside the session's transaction."""
    cursor = session.connection().connection.driver_connection.cursor()
    with cursor.copy(f"COPY {target} FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)


def _copy_upsert(
    session: Session, table: str, columns: str, rows: Iterable[tuple]
) -> None:
    """COPY rows into a temporary staging table, then add the new ones to the idx table.

    The staging table _stage_<table> lives until the transaction ends, so callers can join to it.
    """
    names = ", ".join(c.split()[0] for c in columns.split(","))
    session.execute(
        text(f"CREATE TEMP TABLE _stage_{table} ({columns}) ON COMMIT DROP")
    )
    _copy(session, f"_stage_{table} ({names})", rows)
    session.execute(
        text(
            f"INSERT INTO {INDEX_SCHEMA}.{table} ({names}) "
            f"SELECT DISTINCT {names} FROM _stage_{table} ORDER BY {names} "
            "ON CONFLICT DO NOTHING"  # a fixed order, so concurrent loads cannot deadlock
        )
    )


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

            _copy_upsert(
                session,
                "identifier",
                "subject_id bigint, scheme text, value text, source text",
                ((sid(i.subject), i.scheme, i.value, i.source) for i in identifiers),
            )
            _copy_upsert(
                session,
                "relation",
                "from_id bigint, rel text, to_id bigint",
                ((sid(r.from_subject), r.rel, sid(r.to_subject)) for r in relations),
            )

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
            _copy(
                session,
                f"{INDEX_SCHEMA}.claim (subject_id, mapping_id, run_id)",
                ((s, m, run_row.id) for s, m in sorted(rows)),
            )

            if replaced:  # only replaced claims can leave unused mappings; skipping this keeps parallel loads safe
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
        _copy_upsert(session, "subject", "kind text, accession text", refs)
        rows = session.execute(
            text(
                f"SELECT s.kind, s.accession, s.id FROM {INDEX_SCHEMA}.subject s "
                "JOIN _stage_subject t USING (kind, accession)"
            )
        )
        return {(k, a): i for k, a, i in rows}

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

    def search(
        self,
        organism: list[str] | None = None,
        assay: list[str] | None = None,
        tissue: list[str] | None = None,
        sample_type: list[str] | None = None,
        min_confidence: str = "low",
        match: str = "any",
        limit: int = 100,
        offset: int = 0,
        query_embedding: list[float] | None = None,
    ) -> IndexSearchResult:
        """Find samples by what the index says about them.

        Parameters combine with AND; values within one parameter combine with OR.
        Each value is a term ID or an exact term name (case-insensitive), and also
        matches every term below it (liver finds hepatocyte).

        Args:
            organism: e.g. ['NCBITaxon:9606'] or ['Homo sapiens'].
            assay: e.g. ['EFO:0007045'] or ['ATAC-seq'].
            tissue: Tissue or cell type, e.g. ['UBERON:0002107'] or ['liver'].
            sample_type: e.g. ['tissue', 'organoid'].
            min_confidence: Ignore claims below this level.
            match: 'any' (any claim for a field may match) or 'best' (only the
                sample's highest-confidence claims for that field count).
            limit: Page size.
            offset: Page start.
            query_embedding: Embedding of a text query (all-MiniLM-L6-v2). Results are then
                ranked by similarity to it, and only samples with an embedding are searched.

        Returns:
            Matching samples, ordered by accession (or by similarity, with query_embedding),
            with the claims that matched.

        Raises:
            ValueError: If no parameter is given, or min_confidence or match is unknown.
        """
        if min_confidence not in INDEX_CONFIDENCE_LEVELS:
            raise ValueError(f"Unknown confidence level '{min_confidence}'")
        if match not in INDEX_MATCH_MODES:
            raise ValueError(f"Unknown match mode '{match}'")
        criteria = {
            name: values
            for name, values in (
                ("organism", organism),
                ("assay", assay),
                ("tissue", tissue),
                ("sample_type", sample_type),
            )
            if values
        }
        if not criteria and query_embedding is None:
            raise ValueError(f"Give at least one of: {', '.join(SEARCH_FIELDS)}")
        if query_embedding is not None and match != "any":
            raise ValueError("A text query works with match='any' only")

        with Session(self._sa_engine) as session:
            if match == "any":
                return self._search_facets(
                    session, criteria, min_confidence, limit, offset, query_embedding
                )
            return self._search_claims(
                session, criteria, min_confidence, match, limit, offset
            )

    def _search_claims(
        self,
        session: Session,
        criteria: dict,
        min_confidence: str = "low",
        match: str = "any",
        limit: int = 100,
        offset: int = 0,
    ) -> IndexSearchResult:
        """Search by reading claims directly (used for match='best'; slower than the facets)."""
        matches = []
        for name, values in criteria.items():
            # The mapping table is small: resolve ids here so the claim
            # query gets literal values, which the planner estimates well.
            fields = SEARCH_FIELDS[name]
            mappings = self._find_mappings(session, fields, values, min_confidence)
            matches.append(self._matching_claims(session, fields, mappings, match))

        # Start from the criterion with the fewest matching claims and check the others sample by
        # sample through the claim primary key, instead of intersecting large sets (4M human samples).
        def size(m):
            probe = select(m.c.subject_id).limit(SIZE_PROBE).subquery()
            return session.scalar(select(func.count()).select_from(probe))

        driver, *others = sorted(matches, key=size)
        hits = select(driver.c.subject_id).distinct()
        for m in others:
            hits = hits.where(
                select(m.c.subject_id)
                .where(m.c.subject_id == driver.c.subject_id)
                .exists()
            )
        hits = hits.subquery()
        base = (
            select(IndexSubject.id, IndexSubject.accession)
            .join(hits, hits.c.subject_id == IndexSubject.id)
            .where(IndexSubject.kind == "sample")
        )
        page = session.execute(
            base.add_columns(func.count().over())  # the total, from the same pass
            .order_by(IndexSubject.accession)
            .limit(limit)
            .offset(offset)
        ).all()
        if page:
            total = page[0][2]
        else:  # offset past the end: count separately
            total = session.scalar(select(func.count()).select_from(base.subquery()))
        results = self._search_hits(session, {i: a for i, a, _ in page}, matches)

        return IndexSearchResult(
            total=total, limit=limit, offset=offset, results=results
        )

    def _find_mappings(
        self, session: Session, fields: tuple, values: list[str], min_confidence: str
    ) -> list[tuple[int, str, str]]:
        """(id, field, confidence_level) of mappings matching any of the values."""
        named, lowered = self._named_terms(session, fields, values)
        terms = named | set(
            session.scalars(
                select(IndexTermAncestor.term_id).where(
                    IndexTermAncestor.ancestor_id.in_(named)
                )
            )
        )
        return session.execute(
            select(
                IndexMapping.id, IndexMapping.field, IndexMapping.confidence_level
            ).where(
                IndexMapping.field.in_(fields),
                _rank(IndexMapping.confidence_level) >= _RANK[min_confidence],
                or_(
                    IndexMapping.object_id.in_(terms),
                    # some fields (sample_type) have labels but no term
                    func.lower(IndexMapping.object_label).in_(lowered),
                ),
            )
        ).all()

    def _named_terms(
        self, session: Session, fields: tuple, values: list[str]
    ) -> tuple[set[str], list[str]]:
        """Term IDs the values name (by ID or exact name), and the values lowercased."""
        lowered = [v.lower() for v in values]
        named = set(
            session.scalars(
                union_all(
                    select(IndexTerm.term_id).where(
                        or_(
                            IndexTerm.term_id.in_(values),
                            func.lower(IndexTerm.label).in_(lowered),
                        )
                    ),
                    select(IndexMapping.object_id).where(
                        IndexMapping.field.in_(fields),
                        IndexMapping.object_id.is_not(None),
                        or_(
                            IndexMapping.object_id.in_(values),
                            func.lower(IndexMapping.object_label).in_(lowered),
                        ),
                    ),
                )
            )
        )
        return named, lowered

    def _search_facets(
        self,
        session: Session,
        criteria: dict,
        min_confidence: str,
        limit: int,
        offset: int,
        query_embedding: list[float] | None = None,
    ) -> IndexSearchResult:
        """match='any' search through the sample_facet keys and their GIN index."""
        conditions, page_claims = [], []
        for name, values in criteria.items():
            fields = SEARCH_FIELDS[name]
            named, lowered = self._named_terms(session, fields, values)
            keys = session.scalars(
                select(IndexFacetKey.id).where(
                    IndexFacetKey.field == name,
                    IndexFacetKey.term.in_(
                        list(named) + [f"label:{v}" for v in lowered]
                    ),
                    IndexFacetKey.level >= _RANK[min_confidence],
                )
            ).all()
            if not keys:
                return IndexSearchResult(
                    total=0, limit=limit, offset=offset, results=[]
                )
            conditions.append(IndexSampleFacet.keys.overlap(cast(keys, ARRAY(Integer))))
            below = named | set(
                session.scalars(
                    select(IndexTermAncestor.term_id).where(
                        IndexTermAncestor.ancestor_id.in_(named)
                    )
                )
            )
            page_claims.append((set(fields), below, set(lowered)))

        if query_embedding is not None:
            return self._rank_by_embedding(
                session,
                conditions,
                page_claims,
                min_confidence,
                limit,
                offset,
                query_embedding,
            )
        total = session.scalar(
            select(func.count()).select_from(IndexSampleFacet).where(*conditions)
        )
        if total <= SMALL_RESULT:  # collect the hits, then sort them
            hits = (
                select(IndexSampleFacet.subject_id)
                .where(*conditions)
                .cte("hits")
                .prefix_with("MATERIALIZED")
            )
            statement = select(IndexSubject.id, IndexSubject.accession).join(
                hits, hits.c.subject_id == IndexSubject.id
            )
        else:  # walk samples in accession order and stop after a page of hits
            statement = select(IndexSubject.id, IndexSubject.accession).where(
                IndexSubject.kind == "sample",
                select(IndexSampleFacet.subject_id)
                .where(IndexSampleFacet.subject_id == IndexSubject.id, *conditions)
                .exists(),
            )
        page = dict(
            session.execute(
                statement.order_by(IndexSubject.accession).limit(limit).offset(offset)
            ).all()
        )
        results = self._search_hits(
            session,
            page,
            claims=self._page_claims(session, page, page_claims, min_confidence),
        )
        return IndexSearchResult(
            total=total, limit=limit, offset=offset, results=results
        )

    def _rank_by_embedding(
        self, session, conditions, page_claims, min_confidence, limit, offset, embedding
    ) -> IndexSearchResult:
        """Rank samples that pass the filters by cosine similarity of their description."""
        where = select(IndexSampleFacet.subject_id).where(
            IndexSampleFacet.subject_id == text("st.subject_id"), *conditions
        )
        filtered = (
            str(
                where.exists().compile(
                    session.bind, compile_kwargs={"literal_binds": True}
                )
            )
            if conditions
            else "TRUE"
        )
        vector = "[" + ",".join(f"{x:.6f}" for x in embedding) + "]"
        total = session.scalar(
            text(f"SELECT count(*) FROM {INDEX_SCHEMA}.sample_text st WHERE {filtered}")
        )
        if (
            total > SMALL_RESULT * 2.5
        ):  # many candidates: walk the HNSW index, filtering as it goes
            session.execute(text("SET LOCAL hnsw.iterative_scan = relaxed_order"))
            session.execute(text("SET LOCAL hnsw.ef_search = 200"))
        rows = session.execute(
            text(
                f"""SELECT s.id, s.accession, 1 - (te.embedding <=> CAST(:q AS halfvec)) AS score
                FROM {INDEX_SCHEMA}.text_embedding te
                JOIN {INDEX_SCHEMA}.sample_text st ON st.text_id = te.id
                JOIN {INDEX_SCHEMA}.subject s ON s.id = st.subject_id
                WHERE {filtered}
                ORDER BY te.embedding <=> CAST(:q AS halfvec), s.accession
                LIMIT :limit OFFSET :offset"""
            ),
            {"q": vector, "limit": limit, "offset": offset},
        ).all()
        page = {i: a for i, a, _ in rows}
        hits = self._search_hits(
            session,
            page,
            claims=self._page_claims(session, page, page_claims, min_confidence),
        )
        for hit, (_, _, score) in zip(hits, rows):
            hit.score = round(float(score), 4)
        return IndexSearchResult(total=total, limit=limit, offset=offset, results=hits)

    def _page_claims(self, session, page, page_claims, min_confidence) -> dict:
        """The page's own claims are few: fetch them by subject and keep the ones that matched
        (all of them when there were no label filters)."""
        claims = {}
        for subject_id, m in session.execute(
            select(IndexClaim.subject_id, IndexMapping)
            .distinct()  # a sample in two studies has the same claim from two runs
            .join(IndexMapping, IndexMapping.id == IndexClaim.mapping_id)
            .where(IndexClaim.subject_id.in_(list(page)))
            .order_by(IndexClaim.subject_id, IndexMapping.field, IndexMapping.id)
        ):
            matched = not page_claims or any(
                m.field in fields
                and (m.object_id in below or (m.object_label or "").lower() in lowered)
                for fields, below, lowered in page_claims
            )
            if matched and _RANK[m.confidence_level] >= _RANK[min_confidence]:
                claims.setdefault(subject_id, []).append(m)
        return claims

    def rebuild_facets(self) -> int:
        """Rebuild the search keys (facet_key, sample_facet) from the published claims.

        Run after loading runs; search with match='any' reads only these tables.

        Returns:
            Number of samples with search keys.
        """
        statements = [
            "SET LOCAL work_mem = '256MB'",
            f"TRUNCATE {INDEX_SCHEMA}.sample_facet, {INDEX_SCHEMA}.facet_key RESTART IDENTITY",
            # each mapping -> (search field, term or label, confidence level), plus every ancestor term
            f"""CREATE TEMP TABLE _mapping_key ON COMMIT DROP AS
            WITH m AS (
                SELECT id,
                       CASE WHEN field IN ('tissue', 'cell_type') THEN 'tissue' ELSE field END AS f,
                       COALESCE(object_id, 'label:' || lower(object_label)) AS term,
                       CASE confidence_level WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END AS lvl
                FROM {INDEX_SCHEMA}.mapping
                WHERE field IN ('organism', 'assay', 'tissue', 'cell_type', 'sample_type')),
            t AS (
                SELECT id, f, term, lvl FROM m
                UNION
                SELECT m.id, m.f, a.ancestor_id, m.lvl
                FROM m JOIN {INDEX_SCHEMA}.term_ancestor a ON a.term_id = m.term)
            SELECT id AS mapping_id, f, term, lvl AS level FROM t""",
            f"""INSERT INTO {INDEX_SCHEMA}.facet_key (field, term, level)
            SELECT DISTINCT f, term, level FROM _mapping_key ORDER BY 1, 2, 3""",
            f"""CREATE TEMP TABLE _mapping_key_id ON COMMIT DROP AS
            SELECT x.mapping_id, k.id AS key FROM _mapping_key x
            JOIN {INDEX_SCHEMA}.facet_key k
              ON k.field = x.f AND k.term = x.term AND k.level = x.level""",
            "CREATE INDEX ON _mapping_key_id (mapping_id)",
            f"""INSERT INTO {INDEX_SCHEMA}.sample_facet (subject_id, keys)
            SELECT c.subject_id, array_agg(DISTINCT x.key ORDER BY x.key)
            FROM {INDEX_SCHEMA}.claim c JOIN _mapping_key_id x ON x.mapping_id = c.mapping_id
            GROUP BY c.subject_id""",
        ]
        with Session(self._sa_engine) as session, session.begin():
            for statement in statements:
                session.execute(text(statement))
            return session.scalar(select(func.count()).select_from(IndexSampleFacet))

    def _matching_claims(
        self, session: Session, fields: tuple, mappings: list, match: str
    ):
        """(subject_id, mapping_id) of claims that use the given mappings.

        With match='best', a claim counts only if its subject has no claim of
        higher confidence for the same field.
        """

        def claims(mapping_ids, higher_ids=()):
            statement = select(
                IndexClaim.subject_id.label("subject_id"),
                IndexClaim.mapping_id.label("mapping_id"),
            ).where(IndexClaim.mapping_id.in_(mapping_ids))
            if higher_ids:
                other = aliased(IndexClaim)
                statement = statement.where(
                    ~select(other.subject_id)
                    .where(
                        other.subject_id == IndexClaim.subject_id,
                        other.mapping_id.in_(higher_ids),
                    )
                    .exists()
                )
            return statement

        if match == "any" or not mappings:
            return claims([i for i, _, _ in mappings]).subquery()

        # The mapping table is small, so rank in Python and give Postgres id lists.
        lowest = min(_RANK[level] for _, _, level in mappings)
        others = session.execute(
            select(
                IndexMapping.id, IndexMapping.field, IndexMapping.confidence_level
            ).where(
                IndexMapping.field.in_(fields),
                _rank(IndexMapping.confidence_level) > lowest,
            )
        ).all()
        groups = {}
        for i, field, level in mappings:
            groups.setdefault((field, _RANK[level]), []).append(i)
        return union_all(
            *[
                claims(
                    ids,
                    [i for i, f, lv in others if f == field and _RANK[lv] > rank],
                )
                for (field, rank), ids in groups.items()
            ]
        ).subquery()

    def _search_hits(
        self,
        session: Session,
        page: dict[int, str],
        matches: list = (),
        claims: dict | None = None,
    ) -> list[IndexSearchHit]:
        """Fill in each hit; matched claims come from `matches` subqueries or ready-made `claims`."""
        hits = {i: IndexSearchHit(accession=a) for i, a in page.items()}
        ids = list(hits)

        if claims is None:
            matched = union_all(
                *[select(m.c.subject_id, m.c.mapping_id) for m in matches]
            ).subquery()
            rows = session.execute(
                select(matched.c.subject_id, IndexMapping)
                .distinct()
                .join(IndexMapping, IndexMapping.id == matched.c.mapping_id)
                .where(matched.c.subject_id.in_(ids))
                .order_by(matched.c.subject_id, IndexMapping.field, IndexMapping.id)
            )
        else:
            rows = [(i, m) for i, ms in claims.items() for m in ms]
        for subject_id, m in rows:
            hits[subject_id].claims.append(
                IndexMatchedClaim(
                    **{k: getattr(m, k) for k in IndexMatchedClaim.model_fields}
                )
            )

        study = aliased(IndexSubject)
        for to_id, accession in session.execute(
            select(IndexRelation.to_id, study.accession)
            .join(study, study.id == IndexRelation.from_id)
            .where(IndexRelation.to_id.in_(ids), IndexRelation.rel == "has_sample")
            .order_by(study.accession)
        ):
            hits[to_id].studies.append(accession)

        for subject_id, namespace, name, tag in session.execute(
            select(
                IndexPepLocation.subject_id,
                Projects.namespace,
                Projects.name,
                Projects.tag,
            )
            .join(Projects, Projects.id == IndexPepLocation.project_id)
            .where(IndexPepLocation.subject_id.in_(ids))
            .order_by(Projects.namespace, Projects.name, Projects.tag)
        ):
            hits[subject_id].peps.append(f"{namespace}/{name}:{tag}")

        for subject_id, scheme, value in session.execute(
            select(
                IndexIdentifier.subject_id,
                IndexIdentifier.scheme,
                IndexIdentifier.value,
            )
            .where(IndexIdentifier.subject_id.in_(ids))
            .order_by(IndexIdentifier.scheme, IndexIdentifier.value)
        ):
            hits[subject_id].identifiers.setdefault(scheme, []).append(value)

        return list(hits.values())

    def count_claims(self) -> int:
        with Session(self._sa_engine) as session:
            return session.scalar(select(func.count()).select_from(IndexClaim))
