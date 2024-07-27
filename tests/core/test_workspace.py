import cdf.core.component as cmp
import cdf.core.configuration as conf
import cdf.core.injector as injector
from cdf.core.workspace import Workspace


def test_workspace():
    import dlt

    @dlt.source
    def test_source(a: int, prod_bigquery: str):

        @dlt.resource
        def test_resource():
            yield from [{"a": a, "prod_bigquery": prod_bigquery}]

        return [test_resource]

    # Define a workspace
    datateam = Workspace(
        name="data-team",
        version="0.1.1",
        configuration_sources=[
            # DATATEAM_CONFIG,
            {
                "sfdc": {"username": "abc"},
                "bigquery": {"project_id": ...},
            },
            *Workspace.configuration_sources,
        ],
        service_definitions=[
            cmp.Service(
                "a",
                injector.Dependency(1),
                owner="Alex",
                description="A secret number",
                sla=cmp.ServiceLevelAgreement.CRITICAL,
            ),
            cmp.Service(
                "b", injector.Dependency(lambda a: a + 1 * 5 / 10), owner="Alex"
            ),
            cmp.Service(
                "prod_bigquery", injector.Dependency("dwh-123"), owner="DataTeam"
            ),
            cmp.Service(
                "sfdc",
                injector.Dependency(
                    conf.map_config_section("sfdc")(
                        lambda username: f"https://sfdc.com/{username}"
                    )
                ),
                owner="RevOps",
            ),
        ],
        source_definitions=[
            cmp.Source(
                "source_a",
                injector.Dependency(test_source),
                owner="Alex",
                description="Source A",
            )
        ],
    )

    @conf.map_config_values(secret_number="a.b.c")
    def c(secret_number: int, sfdc: str) -> int:
        print(f"SFDC: {sfdc=}")
        return secret_number * 10

    # Imperatively add dependencies or config if needed
    datateam.add_dependency("c", injector.Dependency(c))
    datateam.import_config({"a.b.c": 10})

    def source_a(a: int, prod_bigquery: str):
        print(f"Source A: {a=}, {prod_bigquery=}")

    # Some interface examples
    assert datateam.name == "data-team"
    datateam.invoke(source_a)
    assert datateam.conf_resolver["sfdc.username"] == "abc"
    assert datateam.container.get_or_raise("sfdc") == "https://sfdc.com/abc"
    assert datateam.invoke(c) == 100
    source = datateam.sources["source_a"]()
    assert list(source) == [{"a": 1, "prod_bigquery": "dwh-123"}]
