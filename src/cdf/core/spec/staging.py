import fnmatch
import functools
import importlib
import typing as t

import pydantic
from sqlglot import exp, parse_one


class StagingRuleset(pydantic.BaseModel, frozen=True):
    """A ruleset for staging models."""

    prefix: str = ""
    """The prefix to apply to all columns."""
    suffix: str = ""
    """The suffix to apply to all columns."""
    excludes: t.Tuple[str, ...] = ()
    """Columns to exclude."""
    includes: t.Tuple[str, ...] = ()
    """Columns to include."""
    predicates: t.Tuple[str, ...] = ()
    """The predicates to apply to the input table."""
    computed_columns: t.Tuple[str, ...] = ()
    """Computed columns to add."""
    join_load_table: bool = False
    """Whether to join the load table."""
    remove_dunders: bool = True
    """Whether to replace double underscores with single underscores."""
    aliasing: t.Tuple[t.Tuple[str, str], ...] = ()
    """
    A map of source table patterns to alias format strings. IE: `("asana_*", "asana_{node.name}")`.

    The first matching aliasing pattern will have its format string applied to all columns in the select statement.
    """
    custom_transforms: t.Tuple[str, ...] = ()
    """Custom transforms to apply."""

    @pydantic.field_validator("aliasing", mode="before")
    @classmethod
    def _validate_aliasing(cls, v: t.Any):
        if isinstance(v, t.Mapping):
            return tuple(v.items())
        return v

    def apply(self, tree: exp.Select, dialect: str | None = None) -> exp.Select:
        """
        Applies the ruleset to a select statement.

        Args:
            select (exp.Select): The select statement to apply the ruleset to. The from must be aliased as "this".
            dialect (str | None, optional): The dialect to use. Defaults to None.

        Returns:
            exp.Select: The transformed select statement.
        """
        prior = tree.meta.get("staging_ruleset")
        if prior is self:
            return tree
        elif prior is None:
            tree.meta["staging_ruleset"] = self
        else:
            tree.meta["staging_ruleset"] += self
        basetable = tree.args["from"].this
        for patt, alias_fmt in self.aliasing:
            if fnmatch.fnmatch(basetable.name, patt):
                for node in tree.selects:
                    column = node.find(exp.Column)
                    assert column
                    if isinstance(node, exp.Alias):
                        node.set(
                            "alias",
                            exp.to_identifier(
                                alias_fmt.format(
                                    node=column,
                                    table=basetable,
                                    **column.this.args,
                                )
                            ),
                        )
                    else:
                        node.replace(
                            node.as_(
                                alias_fmt.format(
                                    node=column,
                                    table=basetable,
                                    **column.this.args,
                                )
                            )
                        )
                break
        origselects = tree.args.pop("expressions")
        tree.args["expressions"] = []
        for c in origselects:
            child = c.find(exp.Column, exp.Alias, exp.Cast)
            assert child
            if isinstance(child, exp.Alias):
                grandchild = child.find(exp.Column)
                assert grandchild
                basename = grandchild.name
                name = child.alias
                if name.startswith("dlt_"):
                    continue
            elif isinstance(child, exp.Cast):
                grandchild = child.find(exp.Column)
                assert grandchild
                basename = name = grandchild.name
            else:
                basename = name = child.name
            if self.excludes and any(
                fnmatch.fnmatch(basename, p) for p in self.excludes
            ):
                continue
            if self.includes and not any(
                fnmatch.fnmatch(basename, p) for p in self.includes
            ):
                continue
            tree = tree.select(c.as_(f"{self.prefix}{name}{self.suffix}"))
        tree = tree.where(*self.predicates)
        for c in self.computed_columns:
            tree = tree.select(parse_one(c, dialect=dialect))
        if self.join_load_table and not tree.meta.get("joined_loads"):
            tree = tree.join(
                exp.table_(
                    exp.to_identifier("_dlt_loads"), basetable.db, basetable.catalog
                ),
                on=exp.column("load_id", "meta").eq(exp.column("_dlt_load_id", "this")),
                join_type="left",
                join_alias="meta",
            ).select(
                exp.cast(
                    exp.column("status", "meta"),
                    exp.DataType.build("text"),
                ).as_("dlt_load_status"),
                exp.cast(
                    exp.column("inserted_at", "meta"),
                    exp.DataType.build("timestamptz"),
                ).as_("dlt_inserted_at"),
                exp.cast(
                    exp.column("schema_version_hash", "meta"),
                    exp.DataType.build("text"),
                ).as_("dlt_schema_version"),
            )
            tree.meta["joined_loads"] = True
        if self.remove_dunders:
            for node in tree.find_all(exp.Alias):
                node.set("alias", exp.to_identifier(node.alias.replace("__", "_")))
        for t_entry in self.custom_transforms:
            t_mod = importlib.import_module(t_entry)
            t_func = getattr(t_mod, "transform")
            tree = t_func(tree)
        return tree

    def __or__(self, other: "StagingRuleset") -> "StagingRuleset":
        """
        Combines two rulesets.

        Args:
            other (StagingRuleset): The other ruleset.

        Returns:
            StagingRuleset: The combined ruleset.
        """
        return StagingRuleset(
            prefix=other.prefix,
            suffix=other.suffix,
            excludes=self.excludes + other.excludes,
            includes=self.includes + other.includes,
            predicates=self.predicates + other.predicates,
            computed_columns=self.computed_columns + other.computed_columns,
            join_load_table=self.join_load_table or other.join_load_table,
            custom_transforms=self.custom_transforms + other.custom_transforms,
        )

    __add__ = __or__

    def merge_with(*rulesets: "StagingRuleset") -> "StagingRuleset":
        """
        Merges multiple rulesets.

        Args:
            *rulesets (StagingRuleset): The rulesets to merge.

        Returns:
            StagingRuleset: The merged ruleset.
        """
        return functools.reduce(lambda a, b: a | b, rulesets)


class StagingSpecification(pydantic.BaseModel, frozen=True):
    """
    Staging specification/DSL for cdf.

    Rulesets are applied in the order they are matched. The output of one rule is the input of the next. Outputs
    are determined based on the output pattern. The default output pattern is `{resource_name}`. Another common
    pattern can be expressed as `stg_{dataset_name}__{table_name}`.
    """

    input: str
    """A glob pattern for the input tables to apply the ruleset to."""
    output: str = "{table.db}_staging.{table.name}"
    """A format string for the output table names."""
    rule: StagingRuleset = StagingRuleset()
    """The ruleset to apply to matches."""
    ignores: t.Tuple[str, ...] = ()
    """A list of patterns to exclude."""

    @functools.lru_cache(maxsize=None)
    def is_applicable(self, table: exp.Table) -> bool:
        """
        Checks if the specification is applicable to a table.

        Args:
            table_name (str): The table name to check.

        Returns:
            bool: Whether the specification is applicable.
        """
        parts = []
        if table.catalog:
            parts.append(table.catalog.strip('"'))
        if table.db:
            parts.append(table.db.strip('"'))
        parts.append(table.name.strip('"'))
        table_name = ".".join(parts)
        return fnmatch.fnmatch(table_name, self.input) and not any(
            fnmatch.fnmatch(table_name, p) for p in self.ignores
        )

    def __call__(self, select: exp.Select) -> t.Tuple[exp.Select, exp.Table]:
        from_ = select.find(exp.Table)
        assert from_, f"No table found in select statement: {select.sql()}"
        return (
            self.rule.apply(select),
            exp.to_table(self.output.format(table=from_, **from_.args)),
        )
