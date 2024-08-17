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
                name="a",
                main=injector.Dependency.instance(1),
                owner="Alex",
                description="A secret number",
                sla=cmp.ServiceLevelAgreement.CRITICAL,
            ),
            cmp.Service(
                name="b",
                main=injector.Dependency.prototype(lambda a: a + 1 * 5 / 10),
                owner="Alex",
            ),
            cmp.Service(
                name="prod_bigquery",
                main=injector.Dependency.instance("dwh-123"),
                owner="DataTeam",
            ),
            cmp.Service(
                name="sfdc",
                main=injector.Dependency(
                    factory=lambda username: f"https://sfdc.com/{username}",
                    config_spec=("sfdc",),
                ),
                owner="RevOps",
            ),
        ],
    )

    @conf.map_config_values(secret_number="a.b.c")
    def c(secret_number: int, sfdc: str) -> int:
        print(f"SFDC: {sfdc=}")
        return secret_number * 10

    # Imperatively add dependencies or config if needed
    datateam.add_dependency("c", injector.Dependency.prototype(c))
    datateam.import_config({"a.b.c": 10})

    def source_a(a: int, prod_bigquery: str):
        print(f"Source A: {a=}, {prod_bigquery=}")

    # Some interface examples
    assert datateam.name == "data-team"
    datateam.invoke(source_a)
    assert datateam.conf_resolver["sfdc.username"] == "abc"
    assert datateam.container.resolve_or_raise("sfdc") == "https://sfdc.com/abc"
    assert datateam.invoke(c) == 100
    assert list(datateam.invoke(test_source)) == [{"a": 1, "prod_bigquery": "dwh-123"}]
