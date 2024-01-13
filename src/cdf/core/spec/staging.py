"""The spec classes for continuous data framework staging automation."""
import fnmatch
import functools
import importlib
import inspect
import re
import typing as t

import pydantic
from sqlglot import exp, parse_one

import cdf.core.logger as logger

T = t.TypeVar("T")


def _ensure(v: T | None) -> T:
    """Ensures a value is not None."""
    if v is None:
        raise ValueError("Got None, expected a value.")
    return v


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
    lstrip: str = ""
    """The prefix to strip from all column names."""
    rstrip: str = ""
    """The suffix to strip from all column names."""
    custom_transforms: t.Tuple[str, ...] = ()
    """Custom transforms to apply."""
    gpt_annotate: bool | str = False
    """Whether to annotate the select statement with OpenAI GPT."""
    sort: bool | str = False
    """Whether to sort the select statement by column name alphabetically."""
    aliasing: t.Tuple[t.Tuple[str, str], ...] = ()
    """
    A map of source table patterns to alias format strings. IE: `("asana_*", "asana_{node.name}")`.

    The first matching aliasing pattern will have its format string applied to all columns in the select statement.
    """

    @pydantic.field_validator("aliasing", mode="before")
    @classmethod
    def _validate_aliasing(cls, v: t.Any):
        if isinstance(v, t.Mapping):
            return tuple(v.items())
        return v

    def apply_projection_filtering_rules(
        self, tree: exp.Select, copy: bool = True
    ) -> exp.Select:
        """
        Applies projection filtering rules to a select statement.

        This method applies the following rules:
        - Removes all projections which are prefixed with "_dlt". (built-in, cannot be disabled)
        - Removes all projections that match one of the exclude patterns. (user-defined)
        - Removes all projections that don't match one of the include patterns. (user-defined)

        Columns are selected in the order they are found in the select statement.
        Projections with multiple columns are not modified.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        if copy:
            tree = tree.copy()

        base_projections = tree.selects.copy()
        tree.set("expressions", [])

        for projection in base_projections:
            column = _ensure(projection.find(exp.Column))
            if column.name.startswith("_dlt"):
                continue
            if self.excludes and any(
                fnmatch.fnmatch(column.name, p) for p in self.excludes
            ):
                continue
            if self.includes and not any(
                fnmatch.fnmatch(column.name, p) for p in self.includes
            ):
                continue
            tree.select(projection, copy=False)

        return tree

    def apply_homogeneous_aliasing(
        self, tree: exp.Select, copy: bool = True
    ) -> exp.Select:
        """
        Applies uniform aliasing rules to a select statement.

        This method applies the following rules:
        - Applies the prefix to all projections. (user-defined)
        - Applies the suffix to all projections. (user-defined)
        - Applies the lstrip to all projections. (user-defined)
        - Applies the rstrip to all projections. (user-defined)

        Columns are selected in the order they are found in the select statement.
        Projections that are aliased have the alias modified. This rule should be applied before any other
        rules since we expect Alias nodes.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        if copy:
            tree = tree.copy()

        base_projections = tree.selects.copy()
        tree.set("expressions", [])

        for projection in base_projections:
            name = projection.alias_or_name
            if name is None:
                inner = projection.find(exp.Column)
                assert inner
                mut_name = inner.name
            mut_name = name.lstrip(self.lstrip).rstrip(self.rstrip)
            if not mut_name.startswith(self.prefix):
                mut_name = self.prefix + mut_name
            if not mut_name.endswith(self.suffix):
                mut_name = mut_name + self.suffix
            ident = exp.to_identifier(mut_name)
            if isinstance(projection, exp.Alias):
                projection.set("alias", ident)
            else:
                projection = projection.as_(ident)
            tree.select(projection, copy=False)

        return tree

    def apply_heterogeneous_aliasing(
        self, tree: exp.Select, copy: bool = True
    ) -> exp.Select:
        """
        Applies f-string based aliasing to a select statement based on the source table and source column.

        This method applies the following rules:
        - Applies the alias format string to each column in the select statement. (user-defined)

        Alias format strings are applied to each column in the select statement. This input is always the base
        column node ignoring any existing aliases. This rule implements a simple aliasing DSL for the format
        string. The following variables are available:
        - `column`: The base column node.
        - `ref`: The source table node.

        This rule takes precedence over the homogeneous aliasing rules as it operates on the base column node.
        Therefore, you cannot use prefix, suffix, lstrip, or rstrip with this rule though each of those rules
        can be expressed in the alias format string due to the finer grain control. The first matching
        aliasing pattern will have its format string applied to all columns in the select statement.
        Subsequent patterns are ignored. Order matters and offers a simple way to implement precedence.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        ref = tree.args["from"].this

        fmt = next((f for p, f in self.aliasing if fnmatch.fnmatch(ref.name, p)), None)
        if fmt is None:
            return tree

        if copy:
            tree = tree.copy()

        for projection in tree.selects:
            column = _ensure(projection.find(exp.Column))
            alias = exp.to_identifier(fmt.format(column=column, ref=ref))
            if isinstance(projection, exp.Alias):
                projection.set("alias", alias)
            else:
                projection.replace(projection.as_(alias))

        return tree

    def apply_computed_columns(
        self, tree: exp.Select, copy: bool = True, dialect: str | None = None
    ) -> exp.Select:
        """
        Applies computed column rules to a select statement.

        This method applies the following rules:
        - Adds all computed columns to the select statement. (user-defined)

        Computed columns are added to the select statement in the order they are defined. They
        must be parseable into an aliased expression for the given dialect.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.
            dialect (str | None, optional): The dialect to use. Defaults to None.

        Returns:
            exp.Select: The transformed select statement.
        """
        if copy:
            tree = tree.copy()

        for projection in self.computed_columns:
            try:
                new_projection = parse_one(projection, dialect=dialect, into=exp.Alias)
            except Exception as e:
                raise ValueError(
                    f"Column must be parseable into an aliased {dialect} expression: {projection}"
                ) from e
            tree.select(new_projection, copy=False)

        return tree

    def apply_join_load_table(self, tree: exp.Select, copy: bool = True) -> exp.Select:
        """
        Applies the join load table rule to a select statement.

        This method applies the following rules:
        - Joins the dlt load table to the select statement. (built-in)

        The dlt load table is joined to the select statement using a left join. The following columns are added:
        - `dlt_load_status`: The status of the load.
        - `dlt_inserted_at`: The timestamp the row was inserted at.
        - `dlt_schema_version`: The schema version hash of the row.

        This rule should only be applied to datasets that are loaded via cdf.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        for node in tree.find_all(exp.Table):
            if node.name == "_dlt_loads":
                return tree

        if copy:
            tree = tree.copy()

        ref = tree.args["from"].this

        if self.join_load_table:
            tree.join(
                exp.table_(exp.to_identifier("_dlt_loads"), ref.db, ref.catalog),
                on=exp.column("load_id", "meta").eq(exp.column("_dlt_load_id", "this")),
                join_type="left",
                join_alias="meta",
                copy=False,
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
                copy=False,
            )

        return tree

    def apply_remove_dunders(self, tree: exp.Select, copy: bool = True) -> exp.Select:
        """
        Applies the remove double underscore rule to a select statement.

        This method applies the following rules:
        - Replaces all double underscores with single underscores. (built-in)

        This rule is mostly relevant for cdf datasets which flatten nested data structures into a single table
        with double underscore separated paths.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        if not self.remove_dunders:
            return tree

        if copy:
            tree = tree.copy()

        for projection in tree.selects:
            clean = projection.alias_or_name.replace("__", "_")
            ident = exp.to_identifier(clean)
            if isinstance(projection, exp.Alias):
                projection.set("alias", ident)
            else:
                projection.replace(projection.as_(ident))

        return tree

    def apply_custom_transforms(
        self, tree: exp.Select, copy: bool = True
    ) -> exp.Select:
        """
        Applies custom transforms to a select statement.

        This method applies the following rules:
        - Applies all custom transforms. (user-defined)

        Custom transforms are applied in the order they are defined. They are entrypoint paths to functions
        which accept and return a select statement. They are applied after all other rules.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        if copy:
            tree = tree.copy()

        for t_entry in self.custom_transforms:
            try:
                t_mod = importlib.import_module(t_entry)
            except ImportError as e:
                raise ValueError(
                    f"Custom transform must be importable: {t_entry}"
                ) from e

            try:
                t_func = getattr(t_mod, "transform")
            except AttributeError as e:
                raise ValueError(
                    f"Custom transform must define a transform function: {t_entry}"
                ) from e

            tree = t_func(tree)

        return tree

    def apply_gpt_annotate(self, tree: exp.Select, copy: bool = True) -> exp.Select:
        """
        Applies GPT annotation to a select statement.

        This method applies the following rules:
        - Annotates the select statement with GPT. (built-in)

        GPT annotation is applied to the select statement using OpenAI's GPT-4 API. Annotations are added as
        comments to the select statement. The LLM makes its best guess at the column descriptions given the context.
        This is meant to be a starting point for data engineers to add their own column descriptions. It works best
        when there are less than 100 columns.

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        if not self.gpt_annotate:
            return tree

        try:
            import openai
        except ImportError as e:
            raise ValueError("openai must be installed to use GPT annotation.") from e

        from sqlmesh.core.dialect import DColonCast

        def cast_to_colon(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Cast) and not node.args.get("format"):
                this = node.this

                if not isinstance(this, (exp.Binary, exp.Unary)) or isinstance(
                    this, exp.Paren
                ):
                    cast = DColonCast(this=this, to=node.to)
                    cast.comments = node.comments
                    node = cast

            exp.replace_children(node, cast_to_colon)
            return node

        query = tree.copy()
        exp.replace_children(query, cast_to_colon)

        if isinstance(self.gpt_annotate, str):
            openai_model = self.gpt_annotate
        else:
            openai_model = "gpt-4-1106-preview"

        logger.info("Annotating select statement with GPT (model: %s).", openai_model)
        completion = openai.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a SQL annotater that annotates SQL queries via comments, reorders columns into "
                        "logical groups, and outputs the updated query. The entire query is always "
                        "returned. Column descriptions are optional and are added as a comment at the end a line. "
                        "If the description cannot be inferred from the context, you should leave it blank. "
                        "Descriptions do not explain the query semantics. Descriptions explain the business meaning "
                        "of the column. Your goal is to add as many column descriptions as possible. You output "
                        "syntactically correct SQL and nothing else."
                    ),
                },
                {
                    "role": "user",
                    "content": inspect.cleandoc(
                        """
                    SELECT
                        id::text as id,
                        created_at::timestamp as created_at,
                        name::text as name,
                        email::text as email,
                        updated_at::timestamp as updated_at,
                        balance::float as balance,
                        abc::text as abc,
                        payments::int as payments,
                        xyz::int as xyz
                    FROM users
                    """
                    ),
                },
                {
                    "role": "assistant",
                    "content": inspect.cleandoc(
                        """
                    SELECT
                        id::text as id,  -- The user id in our dbms
                        name::text as name, -- The user name as it appears in the UI
                        email::text as email, -- The email affiliated with the user

                        payments::int as payments, -- The number of payments the user has made
                        balance::float as balance, -- The number of dollars in the user's account

                        created_at::timestamp as created_at, -- The time the user was created
                        updated_at::timestamp as updated_at -- The last time the user was updated

                        abc::text as abc,
                        xyz::int as xyz
                    FROM users
                    """
                    ),
                },
                {
                    "role": "user",
                    "content": query.sql(pretty=True),
                },
            ],
            model=openai_model,
        )
        maybe_query = completion.choices[0].message.content
        if not maybe_query:
            logger.warning("GPT did not return a query.")
            return tree
        logger.info("GPT query augmentation complete. Parsing annotations...")

        if copy:
            tree = tree.copy()

        def _clean_gpt_output(s: str) -> str:
            if "```" in s:
                bloc = s.split("```")[1]
                if bloc.startswith("sql"):
                    return bloc[3:].strip()
                return bloc.strip()
            return s.strip()

        try:
            annotated_tree = t.cast(
                exp.Select, parse_one(_clean_gpt_output(maybe_query))
            )
        except Exception as e:
            logger.warning("GPT returned an invalid query.", exc_info=e)
            return tree
        logger.info("Applying GPT annotations to select statement.")
        for gpt_projection in annotated_tree.selects:
            gpt_comments = gpt_projection.comments
            if gpt_comments:
                gpt_column = _ensure(gpt_projection.find(exp.Column))
                for projection in tree.selects:
                    column = _ensure(projection.find(exp.Column))
                    if column.name == gpt_column.name:
                        projection.add_comments(gpt_comments)
                        break

        return tree

    def apply_sort(self, tree: exp.Select, copy: bool = True) -> exp.Select:
        """
        Applies the sort rule to a select statement.

        This method applies the following rules:
        - Sorts the select statement by column name alphabetically. (user-defined)

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        if not self.sort:
            return tree

        if copy:
            tree = tree.copy()

        reverse = False
        if isinstance(self.sort, str):
            if self.sort.lower() not in ("asc", "desc"):
                raise ValueError(
                    f"Sort must be one of 'asc' or 'desc', got: {self.sort}"
                )
            reverse = self.sort == "desc"

        tree.set(
            "expressions",
            sorted(tree.expressions, key=lambda e: e.alias_or_name, reverse=reverse),
        )
        return tree

    def apply_predicates(self, tree: exp.Select, copy: bool = True) -> exp.Select:
        """
        Applies predicates to a select statement.

        This method applies the following rules:
        - Applies all predicates. (user-defined)

        Predicates are applied in the order they are defined and multiple predicates are joined with an "and".

        Args:
            tree (exp.Select): The select statement to apply the rules to.
            copy (bool, optional): Whether to copy the select statement before applying the rules. Defaults to True.

        Returns:
            exp.Select: The transformed select statement.
        """
        if not self.predicates:
            return tree

        if copy:
            tree = tree.copy()

        tree.where(*self.predicates, copy=False)
        return tree

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

        tree = self.apply_projection_filtering_rules(tree)
        tree = self.apply_heterogeneous_aliasing(tree)
        tree = self.apply_homogeneous_aliasing(tree)
        tree = self.apply_remove_dunders(tree)
        tree = self.apply_computed_columns(tree, dialect=dialect)
        tree = self.apply_join_load_table(tree)
        tree = self.apply_predicates(tree)
        tree = self.apply_custom_transforms(tree)
        tree = self.apply_gpt_annotate(tree)
        tree = self.apply_sort(tree)

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
        Merges multiple rulesets into a single combined ruleset.

        Args:
            *rulesets (StagingRuleset): The rulesets to merge.

        Returns:
            StagingRuleset: The merged ruleset.
        """
        return functools.reduce(lambda a, b: a | b, rulesets)


class StagingSpecification(pydantic.BaseModel, frozen=True):
    """
    Staging specification/DSL for cdf.

    Staging specs wrap rulesets and apply them to a set of sqlmesh external tables. They are used to generate
    staging models. The input pattern is a glob pattern that matches external model fqns (quotes are stripped
    for simplicity). Outputs are determined based on the output format string. The output pattern is a format
    string which accepts the following variables:
    - `ref`: The source table node.
    - `**ref.args`: The source table node arguments.

    Given the following input pattern: `*.public.*` and the following output pattern: `{ref.db}_staging.{ref.name}`
    the following tables would be matched:
    - `public.users`
    - `public.orders`
    - `public.products`

    The following output tables would be generated:
    - `public_staging.users`
    - `public_staging.orders`
    - `public_staging.products`
    """

    input: str
    """A glob pattern for the input tables to apply the ruleset to."""
    output: str = "{ref.db}_staging.{ref.name}"
    """A format string for the output table names."""
    rule: StagingRuleset = StagingRuleset()
    """The ruleset to apply to matches."""
    ignores: t.Tuple[str, ...] = ()
    """A list of external table patterns to exclude."""

    @functools.lru_cache(maxsize=None)
    def is_applicable(self, ref: exp.Table) -> bool:
        """
        Checks if the specification is applicable to a table.

        Args:
            table_name (str): The table name to check.

        Returns:
            bool: Whether the specification is applicable.
        """
        basic_fqn = ".".join(
            map(lambda t: t.strip('"'), (ref.catalog, ref.db, ref.name))
        )
        return fnmatch.fnmatch(basic_fqn, self.input) and not any(
            fnmatch.fnmatch(basic_fqn, p) for p in self.ignores
        )

    def __call__(self, select: exp.Select) -> t.Tuple[exp.Select, exp.Table]:
        """Applies the specification to a select statement."""
        ref = _ensure(select.find(exp.Table))
        return (
            self.rule.apply(select),
            exp.to_table(self.output.format(ref=ref, **ref.args)),
        )
