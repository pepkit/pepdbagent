import peprs
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from pepdbagent.db_utils import IndexClaim, IndexMapping, IndexPepLocation, Samples
from pepdbagent.models import (
    IndexClaimIn,
    IndexIdentifierIn,
    IndexPepLocationIn,
    IndexRelationIn,
    IndexRunIn,
    IndexSubjectRef,
)

from .utils import PEPDBAgentContextManager, get_path_to_example_file


def claim(accession, field="tissue", object_id="UBERON:0002107", label="liver"):
    return IndexClaimIn(
        subject=IndexSubjectRef(accession=accession),
        field=field,
        subject_label="Liver",
        subject_source="geo:tissue",
        object_id=object_id,
        object_label=label,
        mapping_justification="semapv:LexicalMatching",
        curation_rule="rule:ontology_exact",
        confidence_level="high",
    )


def run(labeler="rules", scope="namespace:namespace1"):
    return IndexRunIn(
        labeler=labeler,
        scope=scope,
        code_version="test",
        mapping_tool="test-tool",
        mapping_tool_version="1",
    )


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestIndex:
    def test_load_and_get_claims(self):
        with PEPDBAgentContextManager(add_data=False) as agent:
            result = agent.index.replace_run(
                run(),
                [
                    claim("GSM1"),
                    claim("GSM1", "organism", "NCBITaxon:9606", "Homo sapiens"),
                ],
                identifiers=[
                    IndexIdentifierIn(
                        subject=IndexSubjectRef(accession="GSM1"),
                        scheme="sra_experiment",
                        value="SRX1",
                    )
                ],
                relations=[
                    IndexRelationIn(
                        from_subject=IndexSubjectRef(kind="study", accession="GSE1"),
                        rel="has_sample",
                        to_subject=IndexSubjectRef(accession="GSM1"),
                    )
                ],
            )
            assert result.claims == 2
            assert result.subjects == 2
            claims = agent.index.get_claims("GSM1")
            assert [c.field for c in claims] == ["organism", "tissue"]
            assert claims[1].object_id == "UBERON:0002107"
            assert claims[1].run_id == result.run_id
            assert claims[1].mapping_tool == "test-tool"

    def test_rerun_replaces_only_its_own_claims(self):
        with PEPDBAgentContextManager(add_data=False) as agent:
            agent.index.replace_run(run(), [claim("GSM1"), claim("GSM2")])
            agent.index.replace_run(run(labeler="model"), [claim("GSM1")])
            agent.index.replace_run(run(scope="namespace:other"), [claim("GSM3")])

            result = agent.index.replace_run(run(), [claim("GSM1")])

            assert result.claims_replaced == 2
            assert agent.index.get_claims("GSM2") == []
            assert len(agent.index.get_claims("GSM1")) == 2  # rules (new) + model
            assert len(agent.index.get_claims("GSM3")) == 1  # other scope untouched

    def test_claims_share_mappings(self):
        with PEPDBAgentContextManager(add_data=False) as agent:

            def count_mappings():
                with Session(agent.pep_db_engine.engine) as session:
                    return session.scalar(
                        select(func.count()).select_from(IndexMapping)
                    )

            result = agent.index.replace_run(
                run(), [claim("GSM1"), claim("GSM2"), claim("GSM2")]
            )
            assert result.claims == 2  # the repeated claim is stored once
            assert count_mappings() == 1
            agent.index.replace_run(run(labeler="model"), [claim("GSM3")])
            assert count_mappings() == 1  # other labelers reuse it

            agent.index.replace_run(
                run(), [claim("GSM1", "tissue", "UBERON:0002048", "lung")]
            )
            assert count_mappings() == 2  # liver kept: the model run still uses it
            agent.index.replace_run(
                run(labeler="model"),
                [claim("GSM3", "tissue", "UBERON:0002048", "lung")],
            )
            assert count_mappings() == 1  # liver is gone once nothing uses it

    def test_invalid_claim_writes_nothing(self):
        with PEPDBAgentContextManager(add_data=False) as agent:
            with pytest.raises(ValueError):
                agent.index.replace_run(run(), [claim("GSM1", field="favorite_color")])
            assert agent.index.count_claims() == 0

    def test_pep_location_survives_project_overwrite(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            location = IndexPepLocationIn(
                subject=IndexSubjectRef(accession="pig_0h"),
                namespace="namespace1",
                name="amendments1",
                tag="default",
                match_key="sample_name",
            )
            missing = location.model_copy(update={"name": "no_such_project"})
            result = agent.index.replace_run(
                run(), [claim("pig_0h")], pep_locations=[location, missing]
            )
            assert result.pep_locations == 1
            assert result.pep_locations_missing == 1

            # GEO refreshes use overwrite, which deletes and recreates sample rows
            agent.project.create(
                project=peprs.Project(
                    get_path_to_example_file("namespace1", "amendments1")
                ),
                namespace="namespace1",
                name="amendments1",
                tag="default",
                overwrite=True,
            )

            with Session(agent.pep_db_engine.engine) as session:
                loc = session.scalars(select(IndexPepLocation)).one()
                raw = session.scalars(
                    select(Samples).where(
                        Samples.project_id == loc.project_id,
                        Samples.sample[loc.match_key].as_string() == "pig_0h",
                    )
                ).all()
                assert len(raw) == 1

            agent.project.delete(
                namespace="namespace1", name="amendments1", tag="default"
            )
            with Session(agent.pep_db_engine.engine) as session:
                assert (
                    session.scalar(select(func.count()).select_from(IndexPepLocation))
                    == 0
                )
                assert session.scalar(select(func.count()).select_from(IndexClaim)) == 1

    def test_hierarchy_search(self):
        with PEPDBAgentContextManager(add_data=False) as agent:
            agent.index.replace_terms(
                terms=[
                    {
                        "term_id": "UBERON:0002107",
                        "label": "liver",
                        "ontology": "UBERON",
                    },
                    {"term_id": "CL:0000182", "label": "hepatocyte", "ontology": "CL"},
                    {
                        "term_id": "UBERON:0002048",
                        "label": "lung",
                        "ontology": "UBERON",
                    },
                ],
                ancestors=[("CL:0000182", "UBERON:0002107")],
                ontologies=["UBERON", "CL"],
            )
            agent.index.replace_run(
                run(),
                [
                    claim("GSM1", "cell_type", "CL:0000182", "hepatocyte"),
                    claim("GSM2", "tissue", "UBERON:0002048", "lung"),
                    claim("GSM3"),
                ],
            )
            query = text(
                """
                SELECT DISTINCT s.accession FROM idx.subject s
                JOIN idx.claim c ON c.subject_id = s.id
                JOIN idx.mapping t ON t.id = c.mapping_id AND t.field IN ('tissue', 'cell_type')
                WHERE t.object_id = :term
                   OR t.object_id IN (SELECT term_id FROM idx.term_ancestor WHERE ancestor_id = :term)
                ORDER BY 1
                """
            )
            with agent.pep_db_engine.engine.connect() as conn:
                found = conn.execute(query, {"term": "UBERON:0002107"}).scalars().all()
            assert found == ["GSM1", "GSM3"]

    def test_search(self):
        with PEPDBAgentContextManager(add_data=False) as agent:
            agent.index.replace_terms(
                terms=[
                    {
                        "term_id": "UBERON:0002107",
                        "label": "liver",
                        "ontology": "UBERON",
                    },
                    {"term_id": "CL:0000182", "label": "hepatocyte", "ontology": "CL"},
                ],
                ancestors=[("CL:0000182", "UBERON:0002107")],
                ontologies=["UBERON", "CL"],
            )
            human = ("organism", "NCBITaxon:9606", "Homo sapiens")
            mouse = ("organism", "NCBITaxon:10090", "Mus musculus")
            lung = ("tissue", "UBERON:0002048", "lung")
            low_liver = claim("GSM4").model_copy(update={"confidence_level": "low"})
            organoid = claim("GSM2", "sample_type", None, "organoid")
            agent.index.replace_run(
                run(),
                [
                    claim("GSM1", *human),
                    claim("GSM1", "cell_type", "CL:0000182", "hepatocyte"),
                    claim("GSM2", *human),
                    claim("GSM2"),
                    organoid,
                    claim("GSM3", *mouse),
                    claim("GSM3"),
                    claim("GSM4", *human),
                    claim("GSM4", *lung),
                    low_liver,
                ],
            )

            assert agent.index.rebuild_facets() == 4  # search keys for GSM1-4

            def found(**kwargs):
                return [h.accession for h in agent.index.search(**kwargs).results]

            # AND across parameters, OR within one; hierarchy; names or ids
            assert found(organism=["Homo sapiens"], tissue=["liver"]) == [
                "GSM1",
                "GSM2",
                "GSM4",
            ]
            assert found(
                organism=["NCBITaxon:9606", "NCBITaxon:10090"], tissue=["CL:0000182"]
            ) == ["GSM1"]
            assert found(tissue=["liver"], sample_type=["Organoid"]) == ["GSM2"]
            assert found(tissue=["liver"], min_confidence="medium") == [
                "GSM1",
                "GSM2",
                "GSM3",
            ]
            # GSM4's lung claim outranks its liver claim
            assert "GSM4" not in found(tissue=["liver"], match="best")
            assert found(tissue=["no such tissue"]) == []

            result = agent.index.search(tissue=["liver"], limit=2, offset=1)
            assert result.total == 4
            assert [h.accession for h in result.results] == ["GSM2", "GSM3"]
            assert [c.field for c in result.results[0].claims] == ["tissue"]

            # the search keys give the same answers as reading the claims directly
            for kwargs in (
                dict(tissue=["liver"]),
                dict(organism=["Homo sapiens"], tissue=["liver"]),
                dict(tissue=["liver"], min_confidence="medium"),
            ):
                criteria = {k: v for k, v in kwargs.items() if k != "min_confidence"}
                level = kwargs.get("min_confidence", "low")
                with Session(agent.pep_db_engine.engine) as session:
                    direct = agent.index._search_claims(session, criteria, level)
                assert found(**kwargs) == [h.accession for h in direct.results]

            with pytest.raises(ValueError):
                agent.index.search()
