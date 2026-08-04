use super::{
    BTreeMap, BTreeSet, DefaultSecretProvider, DependencyTuple, DestinationProtocol,
    DestinationProtocolCapabilities, DestinationSheetArtifact, DurationSpec, EnvSecretProvider,
    ExecutionExtent, FileResourceSourceResolver, FileSecretProvider,
    InMemoryResourceSourceResolver, NORMALIZER_NAMECASE_V1, Path, PathBuf, ProjectScaffoldOptions,
    ResolvedProjectDestination, RetentionRule, SecretProvider, SecretRef, SecretUri,
    SemanticCatalog, SourceDeclaration, TargetName, TypeMappingFidelity, Visit,
    compile_project_declarative_resources, compile_project_declarative_resources_with_root,
    diff_lockfiles, env, fs, generate_lockfile_with_destination_artifacts, lock_to_toml,
    parse_cdf_toml, parse_lock, semantic_hash,
    support::{
        BOOK_PROJECT, GITHUB_RESOURCE, destination_sheet, test_execution_services,
        test_source_registry,
    },
    validate_environment_uri_fields, validate_project, write_local_project_scaffold,
};

pub(super) const SAFETY_WALL_DECISION: &str =
    ".10x/decisions/compiler-enforced-rust-safety-walls.md";

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct UnsafeSyntaxInventory {
    pub(super) allowance_targets: Vec<String>,
    pub(super) unsafe_functions: Vec<String>,
    pub(super) unsafe_function_contracts: Vec<String>,
    pub(super) unsafe_blocks: usize,
    pub(super) unsafe_macro_tokens: usize,
    pub(super) unsafe_foreign_modules: usize,
    pub(super) unsafe_impls: usize,
    pub(super) unsafe_traits: usize,
}

#[derive(Default)]
pub(super) struct UnsafeSyntaxVisitor {
    pub(super) inventory: UnsafeSyntaxInventory,
}

impl UnsafeSyntaxInventory {
    pub(super) fn sort_multisets(&mut self) {
        self.allowance_targets.sort();
        self.unsafe_functions.sort();
        self.unsafe_function_contracts.sort();
    }
}

impl<'ast> Visit<'ast> for UnsafeSyntaxVisitor {
    fn visit_file(&mut self, file: &'ast syn::File) {
        if attributes_allow_unsafe(&file.attrs) {
            self.inventory.allowance_targets.push("crate".to_owned());
        }
        syn::visit::visit_file(self, file);
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if attributes_allow_unsafe(&item.attrs) {
            self.inventory
                .allowance_targets
                .push(format!("mod {}", item.ident));
        }
        syn::visit::visit_item_mod(self, item);
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if attributes_allow_unsafe(&item.attrs) {
            self.inventory
                .allowance_targets
                .push(format!("fn {}", item.sig.ident));
        }
        if item.sig.unsafety.is_some() && has_safety_contract(&item.attrs) {
            self.inventory
                .unsafe_function_contracts
                .push(item.sig.ident.to_string());
        }
        syn::visit::visit_item_fn(self, item);
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if attributes_allow_unsafe(&item.attrs) {
            self.inventory
                .allowance_targets
                .push(format!("impl fn {}", item.sig.ident));
        }
        if item.sig.unsafety.is_some() && has_safety_contract(&item.attrs) {
            self.inventory
                .unsafe_function_contracts
                .push(format!("impl fn {}", item.sig.ident));
        }
        syn::visit::visit_impl_item_fn(self, item);
    }

    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        if attributes_allow_unsafe(&item.attrs) {
            self.inventory
                .allowance_targets
                .push(format!("trait fn {}", item.sig.ident));
        }
        if item.sig.unsafety.is_some() && has_safety_contract(&item.attrs) {
            self.inventory
                .unsafe_function_contracts
                .push(format!("trait fn {}", item.sig.ident));
        }
        syn::visit::visit_trait_item_fn(self, item);
    }

    fn visit_signature(&mut self, signature: &'ast syn::Signature) {
        if signature.unsafety.is_some() {
            self.inventory
                .unsafe_functions
                .push(signature.ident.to_string());
        }
        syn::visit::visit_signature(self, signature);
    }

    fn visit_expr_unsafe(&mut self, expression: &'ast syn::ExprUnsafe) {
        self.inventory.unsafe_blocks += 1;
        syn::visit::visit_expr_unsafe(self, expression);
    }

    fn visit_macro(&mut self, expression: &'ast syn::Macro) {
        self.inventory.unsafe_macro_tokens += count_unsafe_tokens(expression.tokens.clone());
        syn::visit::visit_macro(self, expression);
    }

    fn visit_item_foreign_mod(&mut self, item: &'ast syn::ItemForeignMod) {
        self.inventory.unsafe_foreign_modules += 1;
        syn::visit::visit_item_foreign_mod(self, item);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        if item.unsafety.is_some() {
            self.inventory.unsafe_impls += 1;
        }
        syn::visit::visit_item_impl(self, item);
    }

    fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
        if item.unsafety.is_some() {
            self.inventory.unsafe_traits += 1;
        }
        syn::visit::visit_item_trait(self, item);
    }
}

pub(super) fn count_unsafe_tokens(tokens: proc_macro2::TokenStream) -> usize {
    tokens
        .into_iter()
        .map(|token| match token {
            proc_macro2::TokenTree::Ident(identifier) if identifier == "unsafe" => 1,
            proc_macro2::TokenTree::Group(group) => count_unsafe_tokens(group.stream()),
            proc_macro2::TokenTree::Ident(_)
            | proc_macro2::TokenTree::Punct(_)
            | proc_macro2::TokenTree::Literal(_) => 0,
        })
        .sum()
}

pub(super) fn attributes_allow_unsafe(attributes: &[syn::Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("allow")
            && match &attribute.meta {
                syn::Meta::List(list) => list
                    .tokens
                    .to_string()
                    .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                    .any(|token| token == "unsafe_code"),
                syn::Meta::Path(_) | syn::Meta::NameValue(_) => false,
            }
    })
}

pub(super) fn has_safety_contract(attributes: &[syn::Attribute]) -> bool {
    let documentation = attributes
        .iter()
        .filter_map(|attribute| {
            if !attribute.path().is_ident("doc") {
                return None;
            }
            let syn::Meta::NameValue(value) = &attribute.meta else {
                return None;
            };
            let syn::Expr::Lit(expression) = &value.value else {
                return None;
            };
            let syn::Lit::Str(text) = &expression.lit else {
                return None;
            };
            Some(text.value())
        })
        .collect::<Vec<_>>()
        .join("\n");
    documentation.contains("# Safety") && documentation.contains(SAFETY_WALL_DECISION)
}

pub(super) fn collect_rust_files(directory: &Path, visit: &mut impl FnMut(&Path)) {
    let mut entries = fs::read_dir(directory)
        .unwrap()
        .collect::<std::io::Result<Vec<_>>>()
        .unwrap();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path: PathBuf = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, visit);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            visit(&path);
        }
    }
}

#[derive(Debug)]
struct ProductionUseInventory {
    module: String,
    relative_file: String,
    imports: Vec<Vec<String>>,
    globs: Vec<String>,
}

#[derive(Default)]
struct ProductionUseVisitor {
    imports: Vec<Vec<String>>,
    globs: Vec<String>,
}

impl<'ast> syn::visit::Visit<'ast> for ProductionUseVisitor {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if attributes_are_test_only(&item.attrs) {
            return;
        }
        syn::visit::visit_item_mod(self, item);
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if attributes_are_test_only(&item.attrs) {
            return;
        }
        syn::visit::visit_item_fn(self, item);
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        if attributes_are_test_only(&item.attrs) {
            return;
        }
        let mut leaves = Vec::new();
        flatten_use_tree(&item.tree, &mut Vec::new(), &mut leaves);
        for leaf in leaves {
            if leaf.last().is_some_and(|segment| segment == "*") {
                self.globs.push(leaf.join("::"));
            } else {
                self.imports.push(leaf);
            }
        }
    }
}

#[test]
fn product_runtime_boundary_checker_detects_forbidden_shapes() {
    let syntax = syn::parse_file("use crate::types::*;").unwrap();
    let mut visitor = ProductionUseVisitor::default();
    visitor.visit_file(&syntax);
    assert_eq!(visitor.globs, vec!["crate::types::*"]);

    let graph = BTreeMap::from([
        ("alpha".to_owned(), BTreeSet::from(["beta".to_owned()])),
        ("beta".to_owned(), BTreeSet::from(["alpha".to_owned()])),
    ]);
    assert_eq!(
        first_module_cycle(&graph),
        Some(vec![
            "alpha".to_owned(),
            "beta".to_owned(),
            "alpha".to_owned(),
        ])
    );
}

#[test]
fn product_runtime_module_boundaries_reject_production_globs_and_cycles() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut glob_violations = Vec::new();
    let mut cycle_violations = Vec::new();

    for package in ["cdf-project", "cdf-python", "cdf-runtime"] {
        let source_root = repository_root.join("crates").join(package).join("src");
        let inventories = production_use_inventories(&source_root);
        let modules = inventories
            .iter()
            .filter(|inventory| !inventory.module.is_empty())
            .map(|inventory| inventory.module.clone())
            .collect::<BTreeSet<_>>();
        let root_exports = root_export_owners(&source_root, &modules);
        let mut graph = modules
            .iter()
            .cloned()
            .map(|module| (module, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();

        for inventory in inventories {
            for glob in inventory.globs {
                glob_violations.push(format!(
                    "{package}/{} imports `{glob}`",
                    inventory.relative_file
                ));
            }
            if inventory.module.is_empty() {
                continue;
            }
            for import in inventory.imports {
                if let Some(target) =
                    resolve_import_owner(&inventory.module, &import, &modules, &root_exports)
                    && target != inventory.module
                {
                    graph
                        .get_mut(&inventory.module)
                        .expect("known source module")
                        .insert(target);
                }
            }
        }

        if let Some(cycle) = first_module_cycle(&graph) {
            cycle_violations.push(format!("{package}: {}", cycle.join(" -> ")));
        }
    }

    assert!(
        glob_violations.is_empty(),
        "production wildcard imports are forbidden:\n{}",
        glob_violations.join("\n")
    );
    assert!(
        cycle_violations.is_empty(),
        "direct production module cycles are forbidden:\n{}",
        cycle_violations.join("\n")
    );
}

fn production_use_inventories(source_root: &Path) -> Vec<ProductionUseInventory> {
    let mut inventories = Vec::new();
    collect_rust_files(source_root, &mut |path| {
        let relative = path.strip_prefix(source_root).unwrap();
        if is_test_only_source(relative) {
            return;
        }
        let source = fs::read_to_string(path).unwrap();
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse production module {}: {error}", path.display()));
        let mut visitor = ProductionUseVisitor::default();
        visitor.visit_file(&syntax);
        inventories.push(ProductionUseInventory {
            module: rust_module_name(relative),
            relative_file: relative.to_string_lossy().replace('\\', "/"),
            imports: visitor.imports,
            globs: visitor.globs,
        });
    });
    inventories
}

fn is_test_only_source(relative: &Path) -> bool {
    relative.components().any(|component| {
        let name = component.as_os_str().to_string_lossy();
        name == "tests"
            || name.ends_with("_tests")
            || name == "tests.rs"
            || name.ends_with("_tests.rs")
            || name.starts_with("test_")
    })
}

fn rust_module_name(relative: &Path) -> String {
    let mut parts = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let file = parts.pop().unwrap();
    let stem = file.strip_suffix(".rs").unwrap_or(&file);
    if !matches!(stem, "lib" | "mod") {
        parts.push(stem.to_owned());
    }
    parts.join("::")
}

fn attributes_are_test_only(attributes: &[syn::Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        let syn::Meta::List(meta) = &attribute.meta else {
            return false;
        };
        meta.path.is_ident("cfg")
            && meta
                .parse_args::<syn::Meta>()
                .is_ok_and(|predicate| cfg_requires_test(&predicate))
    })
}

fn cfg_requires_test(predicate: &syn::Meta) -> bool {
    match predicate {
        syn::Meta::Path(path) => path.is_ident("test"),
        syn::Meta::List(list) if list.path.is_ident("all") => list
            .parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            )
            .is_ok_and(|items| items.iter().any(cfg_requires_test)),
        syn::Meta::List(list) if list.path.is_ident("any") => list
            .parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            )
            .is_ok_and(|items| !items.is_empty() && items.iter().all(cfg_requires_test)),
        syn::Meta::List(_) | syn::Meta::NameValue(_) => false,
    }
}

fn flatten_use_tree(tree: &syn::UseTree, prefix: &mut Vec<String>, leaves: &mut Vec<Vec<String>>) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, leaves);
            prefix.pop();
        }
        syn::UseTree::Name(name) => {
            let mut leaf = prefix.clone();
            leaf.push(name.ident.to_string());
            leaves.push(leaf);
        }
        syn::UseTree::Rename(rename) => {
            let mut leaf = prefix.clone();
            leaf.push(rename.ident.to_string());
            leaves.push(leaf);
        }
        syn::UseTree::Glob(_) => {
            let mut leaf = prefix.clone();
            leaf.push("*".to_owned());
            leaves.push(leaf);
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                flatten_use_tree(item, prefix, leaves);
            }
        }
    }
}

fn root_export_owners(source_root: &Path, modules: &BTreeSet<String>) -> BTreeMap<String, String> {
    let source = fs::read_to_string(source_root.join("lib.rs")).unwrap();
    let syntax = syn::parse_file(&source).unwrap();
    let mut owners = BTreeMap::new();
    for item in syntax.items {
        let syn::Item::Use(item) = item else {
            continue;
        };
        if matches!(item.vis, syn::Visibility::Inherited) || attributes_are_test_only(&item.attrs) {
            continue;
        }
        let mut leaves = Vec::new();
        flatten_use_tree(&item.tree, &mut Vec::new(), &mut leaves);
        for leaf in leaves {
            if leaf.len() < 2 || leaf.last().is_some_and(|segment| segment == "*") {
                continue;
            }
            let export = leaf.last().unwrap().clone();
            if let Some(owner) = longest_module_prefix(&leaf[..leaf.len() - 1], modules) {
                owners.insert(export, owner);
            }
        }
    }
    owners
}

fn resolve_import_owner(
    source: &str,
    import: &[String],
    modules: &BTreeSet<String>,
    root_exports: &BTreeMap<String, String>,
) -> Option<String> {
    if import.is_empty() {
        return None;
    }
    let source_parts = source.split("::").collect::<Vec<_>>();
    let mut cursor = 0;
    let mut absolute = Vec::new();
    match import[0].as_str() {
        "crate" => cursor = 1,
        "self" => {
            absolute.extend(source_parts.iter().map(|part| (*part).to_owned()));
            cursor = 1;
        }
        "super" => {
            absolute.extend(source_parts.iter().map(|part| (*part).to_owned()));
            while import.get(cursor).is_some_and(|segment| segment == "super") {
                absolute.pop();
                cursor += 1;
            }
        }
        _ => {
            let mut relative = source_parts
                .iter()
                .map(|part| (*part).to_owned())
                .collect::<Vec<_>>();
            relative.extend(import.iter().cloned());
            if let Some(owner) = longest_module_prefix(&relative, modules) {
                return Some(owner);
            }
        }
    }
    absolute.extend(import[cursor..].iter().cloned());
    if let Some(owner) = longest_module_prefix(&absolute, modules) {
        return Some(owner);
    }
    (absolute.len() == 1)
        .then(|| root_exports.get(&absolute[0]).cloned())
        .flatten()
}

fn longest_module_prefix(parts: &[String], modules: &BTreeSet<String>) -> Option<String> {
    (1..=parts.len()).rev().find_map(|end| {
        let candidate = parts[..end].join("::");
        modules.contains(&candidate).then_some(candidate)
    })
}

fn first_module_cycle(graph: &BTreeMap<String, BTreeSet<String>>) -> Option<Vec<String>> {
    fn visit(
        node: &str,
        graph: &BTreeMap<String, BTreeSet<String>>,
        complete: &mut BTreeSet<String>,
        active: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        if let Some(start) = active.iter().position(|candidate| candidate == node) {
            let mut cycle = active[start..].to_vec();
            cycle.push(node.to_owned());
            return Some(cycle);
        }
        if complete.contains(node) {
            return None;
        }
        active.push(node.to_owned());
        if let Some(targets) = graph.get(node) {
            for target in targets {
                if let Some(cycle) = visit(target, graph, complete, active) {
                    return Some(cycle);
                }
            }
        }
        active.pop();
        complete.insert(node.to_owned());
        None
    }

    let mut complete = BTreeSet::new();
    for module in graph.keys() {
        let mut active = Vec::new();
        if let Some(cycle) = visit(module, graph, &mut complete, &mut active) {
            return Some(cycle);
        }
    }
    None
}

#[test]
fn workspace_safety_lint_policy_and_exception_set_are_closed() {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workspace: toml::Value =
        toml::from_str(&fs::read_to_string(repository_root.join("Cargo.toml")).unwrap()).unwrap();
    assert_eq!(
        workspace["workspace"]["lints"]["rust"]["unsafe_code"].as_str(),
        Some("deny")
    );
    assert_eq!(
        workspace["workspace"]["lints"]["clippy"]["undocumented_unsafe_blocks"].as_str(),
        Some("deny")
    );

    let members = workspace["workspace"]["members"].as_array().unwrap();
    assert_eq!(
        members.len(),
        53,
        "update the closed workspace-member count"
    );
    for member in members {
        let member = member.as_str().unwrap();
        let manifest: toml::Value = toml::from_str(
            &fs::read_to_string(repository_root.join(member).join("Cargo.toml")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            manifest["lints"]["workspace"].as_bool(),
            Some(true),
            "{member} must explicitly inherit workspace lints"
        );
    }

    let benchmark_unsafe_functions = vec![
        "bind_duckdb_ipc_table_function".to_owned(),
        "drop_duckdb_ipc_table_function_context".to_owned(),
        "drop_duckdb_ipc_table_function_local_state".to_owned(),
        "duckdb_error_data_message_take".to_owned(),
        "duckdb_ipc_table_function_context".to_owned(),
        "duckdb_ipc_table_function_local_state".to_owned(),
        "init_duckdb_ipc_table_function".to_owned(),
        "local_init_duckdb_ipc_table_function".to_owned(),
        "scan_duckdb_ipc_table_function".to_owned(),
    ];
    let duckdb_unsafe_functions = vec![
        "bind".to_owned(),
        "context".to_owned(),
        "drop_context".to_owned(),
        "drop_local_state".to_owned(),
        "init".to_owned(),
        "local_init".to_owned(),
        "local_state".to_owned(),
        "scan".to_owned(),
    ];
    let expected_inventories = BTreeMap::from([
        (
            "crates/cdf-benchmarks/src/lib.rs".to_owned(),
            UnsafeSyntaxInventory {
                allowance_targets: vec!["mod references".to_owned()],
                ..UnsafeSyntaxInventory::default()
            },
        ),
        (
            "crates/cdf-benchmarks/src/references.rs".to_owned(),
            UnsafeSyntaxInventory {
                unsafe_functions: benchmark_unsafe_functions.clone(),
                unsafe_function_contracts: benchmark_unsafe_functions,
                unsafe_blocks: 52,
                ..UnsafeSyntaxInventory::default()
            },
        ),
        (
            "crates/cdf-dest-duckdb/src/ingest_envelope.rs".to_owned(),
            UnsafeSyntaxInventory {
                allowance_targets: vec!["fn estimate_worker_bytes".to_owned()],
                unsafe_blocks: 1,
                ..UnsafeSyntaxInventory::default()
            },
        ),
        (
            "crates/cdf-dest-duckdb/src/lib.rs".to_owned(),
            UnsafeSyntaxInventory {
                allowance_targets: vec!["mod segment_scan".to_owned()],
                ..UnsafeSyntaxInventory::default()
            },
        ),
        (
            "crates/cdf-dest-duckdb/src/segment_scan.rs".to_owned(),
            UnsafeSyntaxInventory {
                unsafe_functions: duckdb_unsafe_functions.clone(),
                unsafe_function_contracts: duckdb_unsafe_functions,
                unsafe_blocks: 40,
                ..UnsafeSyntaxInventory::default()
            },
        ),
        (
            "crates/cdf-python/src/arrow_capsule.rs".to_owned(),
            UnsafeSyntaxInventory {
                unsafe_blocks: 4,
                unsafe_macro_tokens: 2,
                ..UnsafeSyntaxInventory::default()
            },
        ),
        (
            "crates/cdf-python/src/lib.rs".to_owned(),
            UnsafeSyntaxInventory {
                allowance_targets: vec!["mod arrow_capsule".to_owned()],
                ..UnsafeSyntaxInventory::default()
            },
        ),
        (
            "crates/cdf-subprocess/src/runner.rs".to_owned(),
            UnsafeSyntaxInventory {
                allowance_targets: vec!["fn install_child_address_space_limit".to_owned()],
                unsafe_blocks: 1,
                ..UnsafeSyntaxInventory::default()
            },
        ),
    ]);
    let mut actual_inventories = BTreeMap::new();
    collect_rust_files(&repository_root.join("crates"), &mut |path| {
        let source = fs::read_to_string(path).unwrap();
        let relative = path
            .strip_prefix(&repository_root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if relative == "crates/cdf-project/src/tests.rs" {
            return;
        }
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse Rust safety inventory for {relative}: {error}"));
        let mut visitor = UnsafeSyntaxVisitor::default();
        visitor.visit_file(&syntax);
        visitor.inventory.sort_multisets();
        if visitor.inventory != UnsafeSyntaxInventory::default() {
            assert!(
                source.contains(SAFETY_WALL_DECISION),
                "{relative} must cite the governing safety-wall decision"
            );
            actual_inventories.insert(relative, visitor.inventory);
        }
    });
    assert_eq!(actual_inventories, expected_inventories);

    for package in [
        "cdf-kernel",
        "cdf-memory",
        "cdf-runtime",
        "cdf-package",
        "cdf-package-contract",
        "cdf-engine",
        "cdf-task-store",
        "cdf-object-access",
    ] {
        let root = fs::read_to_string(
            repository_root
                .join("crates")
                .join(package)
                .join("src/lib.rs"),
        )
        .unwrap();
        assert!(
            root.contains("clippy::expect_used") && root.contains("clippy::unwrap_used"),
            "{package} must deny production unwrap/expect"
        );
    }
}

#[test]
fn project_normal_build_graph_has_no_concrete_destination_crates() {
    let manifest: toml::Value = toml::from_str(include_str!("../../Cargo.toml")).unwrap();
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .unwrap();
    let concrete = dependencies
        .keys()
        .filter(|name| name.starts_with("cdf-dest-"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        concrete.is_empty(),
        "cdf-project normal dependencies must remain destination-neutral: {concrete:?}"
    );
}

#[test]
fn resolved_destination_binding_configures_direct_runtime_services() {
    let temp = tempfile::tempdir().unwrap();
    let execution = test_execution_services();
    let spill = execution.spill();
    assert_eq!(spill.snapshot().current_bytes, 0);

    {
        let mut destination = ResolvedProjectDestination::new(
            Box::new(
                cdf_dest_duckdb::DuckDbDestination::new(temp.path().join("direct.duckdb")).unwrap(),
            ),
            TargetName::new("events").unwrap(),
        );
        destination
            .bind_execution_services(execution.clone())
            .unwrap();
        assert!(
            spill.snapshot().current_bytes > 0,
            "binding execution services must let direct runtimes reserve native scratch through the shared spill authority"
        );
    }

    assert_eq!(spill.snapshot().current_bytes, 0);
}

#[test]
fn book_project_shape_parses_into_typed_models() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();

    assert_eq!(config.project.name, "acme_data");
    assert_eq!(
        config.driver_options["python"]["interpreter"],
        ".venv/bin/python"
    );
    assert_eq!(config.defaults.contract.as_deref(), Some("governed"));
    assert_eq!(
        config.resources["events.raw"]
            .freshness
            .as_ref()
            .unwrap()
            .alert_after
            .unwrap()
            .millis(),
        2_700_000
    );
    assert_eq!(
        config.environments["dev"]
            .retention
            .as_ref()
            .unwrap()
            .default,
        Some(RetentionRule::Runs(5))
    );
}

#[test]
fn environment_overlays_inherit_unspecified_settings() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(prod.state, "sqlite://.cdf/state.db");
    assert_eq!(prod.packages, ".cdf/packages");
    assert_eq!(prod.destination, "postgres://secret://env/PROD_DWH");
    assert_eq!(
        prod.retention.as_ref().unwrap().default,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            90 * 86_400_000
        )))
    );
    assert_eq!(
        prod.retention.as_ref().unwrap().financial,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            400 * 86_400_000
        )))
    );
}

#[test]
fn destination_policy_overlays_from_default_environment() {
    let project = BOOK_PROJECT
        .replace(
            "retention = { default = \"5 runs\" }\n\n",
            "retention = { default = \"5 runs\" }\n\n[environments.dev.destination_policy.clickhouse]\nmerge_mode = \"replacing_merge_tree\"\n\n",
        );
    let config = parse_cdf_toml(&project).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(
        cdf_runtime::DestinationPolicyProvider::value(
            &prod.destination_policy,
            "clickhouse",
            "merge_mode"
        ),
        Some("replacing_merge_tree")
    );
}

#[test]
fn removed_postgres_merge_dedup_policy_is_rejected() {
    let project = BOOK_PROJECT.replace(
        "retention = { default = \"5 runs\" }\n",
        "retention = { default = \"5 runs\" }\n\n[environments.dev.destination_policy.postgres]\nmerge_dedup = \"fail\"\n",
    );

    let error = parse_cdf_toml(&project).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(
        error
            .message
            .contains("unsupported destination_policy.postgres")
    );
}

#[test]
fn clickhouse_merge_mode_policy_uses_the_ratified_environment_shape() {
    let project = BOOK_PROJECT.to_owned()
        + "\n[environments.prod.destination_policy.clickhouse]\nmerge_mode = \"atomic_copy_on_write\"\n";
    let config = parse_cdf_toml(&project).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(
        cdf_runtime::DestinationPolicyProvider::value(
            &prod.destination_policy,
            "clickhouse",
            "merge_mode"
        ),
        Some("atomic_copy_on_write")
    );
}

#[test]
fn validation_resolves_declarative_sources_and_redacts_secret_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = DefaultSecretProvider::new(
        EnvSecretProvider::from_map([
            ("GITHUB_TOKEN", "github-token-value"),
            ("PROD_DWH", "postgres-dsn-value"),
        ]),
        FileSecretProvider::without_root(),
    );

    let report = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap();

    assert_eq!(report.declarative_resources, 1);
    assert_eq!(report.external_resources, 1);
    assert_eq!(report.checked_secrets.len(), 2);
    let debug = format!("{report:?}");
    assert!(!debug.contains("github-token-value"));
    assert!(!debug.contains("postgres-dsn-value"));
    assert!(debug.contains("secret://env/GITHUB_TOKEN"));
}

#[test]
fn validation_checks_missing_secret_without_printing_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = EnvSecretProvider::from_map([("GITHUB_TOKEN", "github-token-value")]);

    let error = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap_err();

    assert!(error.to_string().contains("secret://env/PROD_DWH"));
    assert!(!error.to_string().contains("github-token-value"));
}

#[test]
fn plaintext_secret_values_are_rejected_where_references_are_required() {
    let bad_resource = GITHUB_RESOURCE.replace("secret://env/GITHUB_TOKEN", "plain-token-value");
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", bad_resource);
    let provider = EnvSecretProvider::from_map([("PROD_DWH", "postgres-dsn-value")]);

    let error = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap_err();

    assert!(error.to_string().contains("secret://"), "{error}");
    assert!(!error.to_string().contains("plain-token-value"));
}

#[test]
fn file_secret_provider_resolves_without_exposing_contents() {
    let root = env::temp_dir().join(format!("cdf-project-secret-test-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("api-token"), "file-secret-value\n").unwrap();
    let provider = FileSecretProvider::new(&root);
    let uri = SecretUri::new("secret://file/api-token").unwrap();

    let value = provider.resolve(&uri).unwrap();

    assert_eq!(value.as_str().unwrap(), "file-secret-value");
    assert_eq!(format!("{value:?}"), "[REDACTED]");
    assert_eq!(format!("{value}"), "[REDACTED]");
    let _ = fs::remove_file(root.join("api-token"));
    let _ = fs::remove_dir(root);
}

#[test]
fn lockfile_generation_round_trips_and_diffs_semantic_changes() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();
    let sheet = destination_sheet("duckdb", TypeMappingFidelity::Lossless);
    let sheet_artifact =
        DestinationSheetArtifact::new(sheet.clone(), DestinationProtocolCapabilities::default())
            .unwrap();
    let dependency_tuple = DependencyTuple {
        cdf: "0.1.0".to_owned(),
        arrow_rs: "58.3.0".to_owned(),
        datafusion: Some("54.0.0".to_owned()),
        object_store: None,
        duckdb_rs: None,
        rust: None,
    };

    let lock = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple.clone(),
        std::slice::from_ref(&sheet_artifact),
        BTreeMap::new(),
        &SemanticCatalog::builtins().unwrap(),
    )
    .unwrap();
    let encoded = lock_to_toml(&lock).unwrap();
    assert!(encoded.contains("protocol_capabilities"));
    assert!(encoded.contains("corrections"));
    let decoded = parse_lock(&encoded).unwrap();
    assert_eq!(decoded, lock);
    assert_eq!(lock_to_toml(&decoded).unwrap(), encoded);
    let old_version = encoded.replacen("version = 2", "version = 1", 1);
    let error = parse_lock(&old_version).unwrap_err();
    assert!(error.message.contains("unsupported cdf.lock version"));
    assert_eq!(lock.normalizer, NORMALIZER_NAMECASE_V1);
    let resource = lock.resources.get("github.issues").unwrap();
    assert!(resource.capability_sheet_hash.starts_with("sha256:"));
    assert_eq!(resource.execution_extent, ExecutionExtent::bounded());
    assert!(resource.execution_extent_hash.is_none());
    assert!(resource.compiled_stream_policy.is_none());
    assert!(!encoded.contains("execution_extent"));
    assert!(!encoded.contains("compiled_stream_policy"));
    let mut tampered_lock = lock.clone();
    tampered_lock
        .resources
        .get_mut("github.issues")
        .unwrap()
        .execution_extent_hash = Some(format!("sha256:{}", "00".repeat(32)));
    assert!(
        lock_to_toml(&tampered_lock)
            .unwrap_err()
            .message
            .contains("execution-extent hash")
    );
    assert!(
        resource
            .schema_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    let contract = resource.contract.as_ref().unwrap();
    assert!(
        contract
            .policy_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        contract
            .validation_program_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(
        lock.destinations["duckdb"].sheet.type_mappings[0].fidelity,
        TypeMappingFidelity::Lossless
    );
    assert_eq!(
        lock.destinations["duckdb"].sheet_hash,
        semantic_hash(&sheet_artifact).unwrap()
    );

    let changed_sheet = destination_sheet(
        "duckdb",
        TypeMappingFidelity::LossyRequiresContractAllowance,
    );
    let changed_artifact =
        DestinationSheetArtifact::new(changed_sheet, DestinationProtocolCapabilities::default())
            .unwrap();
    let changed = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple.clone(),
        &[changed_artifact],
        BTreeMap::new(),
        &SemanticCatalog::builtins().unwrap(),
    )
    .unwrap();
    let diffs = diff_lockfiles(&lock, &changed).unwrap();

    assert!(diffs.iter().any(|diff| diff.path.contains("sheet_hash")));
    assert!(diffs.iter().any(|diff| {
        diff.path
            .contains("destinations.duckdb.sheet.type_mappings")
    }));

    let postgres_artifact = cdf_dest_postgres::PostgresDestination::new()
        .sheet_artifact()
        .unwrap();
    let parquet_temp = tempfile::tempdir().unwrap();
    let parquet_artifact = cdf_dest_parquet::ParquetDestination::new_filesystem(
        parquet_temp.path(),
        test_execution_services(),
    )
    .unwrap()
    .sheet_artifact()
    .unwrap();
    let typed_lock = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple,
        &[postgres_artifact.clone(), parquet_artifact.clone()],
        BTreeMap::new(),
        &SemanticCatalog::builtins().unwrap(),
    )
    .unwrap();
    let typed_encoded = lock_to_toml(&typed_lock).unwrap();
    assert!(typed_encoded.contains("protocol_capabilities"));
    assert!(typed_encoded.contains("corrections"));
    assert!(typed_encoded.contains("object_key_rules"));
    assert!(typed_encoded.contains("object-key-component-v1"));
    let typed_decoded = parse_lock(&typed_encoded).unwrap();
    assert_eq!(typed_decoded, typed_lock);
    assert_eq!(lock_to_toml(&typed_decoded).unwrap(), typed_encoded);
    assert_eq!(
        typed_lock.destinations["postgres"]
            .sheet_artifact()
            .unwrap(),
        postgres_artifact
    );
    assert_eq!(
        typed_lock.destinations["parquet_object_store"]
            .sheet_artifact()
            .unwrap(),
        parquet_artifact
    );
}

#[test]
fn inline_uri_credentials_are_rejected() {
    let input = BOOK_PROJECT.replace(
        "destination = \"duckdb://.cdf/dev.duckdb\"",
        "destination = \"postgres://user:password@example.com/db\"",
    );
    let config = parse_cdf_toml(&input).unwrap();

    let error = config.effective_environment("dev").and_then(|env| {
        validate_environment_uri_fields(&env)?;
        Ok(())
    });

    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("inline credentials")
    );
}

#[test]
fn secret_ref_requires_provider_and_key() {
    assert!(SecretRef::new("secret://env/TOKEN").is_ok());
    assert!(SecretRef::new("env:TOKEN").is_err());
    assert!(SecretRef::new("secret://env").is_err());
}

#[test]
fn declarative_resource_compilation_hook_uses_cdf_declarative() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);

    let resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();

    assert_eq!(resources.len(), 1);
    assert_eq!(
        resources[0].descriptor().resource_id.as_str(),
        "github.issues"
    );
}

#[test]
fn declarative_resource_mapping_pattern_must_match_compiled_id() {
    let project = r#"
[project]
name = "tlc"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."yellow"]
source = "resources/tlc.toml"
"#;
    let resource = r#"
[source.tlc]
kind = "files"
root = "data"

[resource.yellow]
glob = "*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/tlc.toml", resource);

    let error = compile_project_declarative_resources(&test_source_registry(), &config, &resolver)
        .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("resource mapping pattern `yellow`"));
    assert!(message.contains("tlc.yellow"));
    assert!(message.contains("`<source>.<resource>`"));
    assert!(message.contains("[resources.\"tlc.yellow\"]"));
}

#[test]
fn declarative_file_roots_resolve_under_project_root_for_runtime_compile() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join("resources")).unwrap();
    let project = r#"
[project]
name = "files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#;
    let resource = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
"#;
    fs::write(temp.path().join("resources/files.toml"), resource).unwrap();
    let config = parse_cdf_toml(project).unwrap();
    let resolver = FileResourceSourceResolver::new(temp.path());

    let resources = compile_project_declarative_resources_with_root(
        &test_source_registry(),
        &config,
        &resolver,
        temp.path(),
    )
    .unwrap();

    assert_eq!(
        resources[0].source_plan().physical_plan["source"]["root"],
        "data"
    );
    assert_eq!(resources[0].project_root(), Some(temp.path()));
}

#[test]
fn local_project_scaffold_writes_valid_project_without_runtime_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("fresh-project");

    let report = write_local_project_scaffold(ProjectScaffoldOptions {
        root: root.clone(),
        project_name: None,
        force: false,
    })
    .unwrap();

    assert_eq!(report.project_name, "fresh-project");
    assert_eq!(
        report.created,
        vec![
            "cdf.toml",
            "README.md",
            ".gitignore",
            "resources",
            "resources/files.toml",
            "data"
        ]
    );
    assert!(root.join("cdf.toml").is_file());
    assert!(root.join("README.md").is_file());
    assert_eq!(
        fs::read_to_string(root.join(".gitignore")).unwrap(),
        ".cdf/\n"
    );
    assert!(
        !fs::read_to_string(root.join(".gitignore"))
            .unwrap()
            .contains("cdf.lock")
    );
    assert!(root.join("resources/files.toml").is_file());
    assert!(root.join("data").is_dir());
    assert!(fs::read_dir(root.join("data")).unwrap().next().is_none());
    assert!(!root.join(".cdf").exists());
    assert!(!root.join("cdf.lock").exists());

    let config = parse_cdf_toml(&fs::read_to_string(root.join("cdf.toml")).unwrap()).unwrap();
    let readme = fs::read_to_string(root.join("README.md")).unwrap();
    let resource = fs::read_to_string(root.join("resources/files.toml")).unwrap();
    assert!(readme.contains("docs/quickstart.md"));
    assert!(readme.contains("cdf validate"));
    assert!(readme.contains("cdf compile --refresh"));
    assert!(readme.contains("manifest_resources"));
    assert!(readme.contains("cdf plan local.events"));
    assert!(readme.contains("cdf run local.events"));
    assert!(!readme.contains("secret://"));
    assert!(!readme.contains(root.to_str().unwrap()));
    assert!(!resource.contains("primary_key"));
    assert!(!resource.contains("merge_key"));
    let resolver = FileResourceSourceResolver::new(&root);
    let provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());
    let validation = validate_project(
        &test_source_registry(),
        &config,
        Some("dev"),
        &resolver,
        &provider,
    )
    .unwrap();

    assert_eq!(validation.declarative_resources, 1);
    assert!(validation.checked_secrets.is_empty());
}

#[test]
fn declarative_postgres_secret_is_collected_for_validation() {
    let project = BOOK_PROJECT.replace(
        "[resources.\"github.*\"]\nsource = \"resources/github.toml\"",
        "[resources.\"warehouse.*\"]\nsource = \"resources/postgres.toml\"",
    );
    let postgres_resource = r#"
[source.warehouse]
kind = "postgres"
connection = "secret://env/POSTGRES_URL"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "merge"
trust = "governed"
"#;
    let config = parse_cdf_toml(&project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new()
        .with_toml("resources/postgres.toml", postgres_resource);
    let provider = EnvSecretProvider::from_map([
        ("POSTGRES_URL", "postgres-url-value"),
        ("PROD_DWH", "postgres-dsn-value"),
    ]);

    let report = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap();

    assert!(
        report
            .checked_secrets
            .iter()
            .any(|check| check.uri.as_str() == "secret://env/POSTGRES_URL")
    );
    assert!(!format!("{report:?}").contains("postgres-url-value"));
}

#[test]
fn unsupported_keychain_provider_is_explicit_not_guessy() {
    let provider = DefaultSecretProvider::default();
    let uri = SecretUri::new("secret://keychain/prod-token").unwrap();
    let error = provider.resolve(&uri).unwrap_err();

    assert!(error.to_string().contains("not available"));
    assert!(!error.to_string().contains("prod-token-value"));
}

#[test]
fn source_declaration_is_registry_open_and_preserves_secret_references() {
    let source = SourceDeclaration {
        kind: "external_api".to_owned(),
        options: BTreeMap::from([(
            "token".to_owned(),
            serde_json::Value::String("secret://env/TOKEN".to_owned()),
        )]),
    };

    assert_eq!(source.kind, "external_api");
    assert_eq!(source.options["token"], "secret://env/TOKEN");
}
