<div align="center">
<h1 align="center">
<img src="https://raw.githubusercontent.com/PKief/vscode-material-icon-theme/ec559a9f6bfd399b82bb44393651661b08aaf7ba/icons/folder-markdown-open.svg" width="100" />
<br>CDF</h1>
<h3>◦ Cdf: Code, Deploy, Flourish-Unleash Your Dev Potential!</h3>
<h3>◦ Developed with the software and tools below.</h3>

<p align="center">
<img src="https://img.shields.io/badge/YAML-CB171E.svg?style=flat-square&logo=YAML&logoColor=white" alt="YAML" />
<img src="https://img.shields.io/badge/Python-3776AB.svg?style=flat-square&logo=Python&logoColor=white" alt="Python" />
<img src="https://img.shields.io/badge/JSON-000000.svg?style=flat-square&logo=JSON&logoColor=white" alt="JSON" />
</p>
<img src="https://img.shields.io/github/license/z3z1ma/cdf?style=flat-square&color=5D6D7E" alt="GitHub license" />
<img src="https://img.shields.io/github/last-commit/z3z1ma/cdf?style=flat-square&color=5D6D7E" alt="git-last-commit" />
<img src="https://img.shields.io/github/commit-activity/m/z3z1ma/cdf?style=flat-square&color=5D6D7E" alt="GitHub commit activity" />
<img src="https://img.shields.io/github/languages/top/z3z1ma/cdf?style=flat-square&color=5D6D7E" alt="GitHub top language" />
</div>

---

## 📖 Table of Contents
- [📖 Table of Contents](#-table-of-contents)
- [📍 Overview](#-overview)
- [📦 Features](#-features)
- [📂 repository Structure](#-repository-structure)
- [⚙️ Modules](#modules)
- [🚀 Getting Started](#-getting-started)
    - [🔧 Installation](#-installation)
    - [🤖 Running cdf](#-running-cdf)
    - [🧪 Tests](#-tests)
- [🛣 Roadmap](#-roadmap)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)
- [👏 Acknowledgments](#-acknowledgments)

---

## 📍 Overview

The Continuous Data Framework (cdf) is a transformative Python-based framework
purpose-built for data platform engineers and teams seeking to revolutionize
their data integration and deployment workflows. At its core, cdf harmonizes
best-in-class open source tools like dlt and sqlmesh, crafting a user-friendly
yet robust interface for crafting and managing data pipelines. This
forward-thinking framework excels in fostering a unified data platform,
simplifying and enhancing the processes for data scientists and engineers
alike.

With its comprehensive CLI, cdf empowers users to effortlessly manage
workspaces, each a hub for intricate pipelines that encompass data retrieval,
transformation, and publishing. The framework is intelligently designed with
feature flags, advanced configuration management, and a Python API, making it
an essential tool for creating, managing, and executing sophisticated data
pipelines. Its keen emphasis on workspace customization and command-line
utilities for project initialization, metadata handling, data transformation,
and external publishing, positions cdf as the go-to solution for collaborative
development in multi-user environments. Each workspace supports individual
configurations, encouraging teamwork while maintaining the integrity of
pipeline versioning.

In essence, cdf is not just a framework but a paradigm shift in continuous data
integration and deployment, offering an unparalleled experience to teams
dedicated to streamlining their data operations.

More comprehensive documentation is available at https://z3z1ma.github.io/cdf. (wip)

## 📦 Features


|    | Feature                  | Description                                                                                                        |
|----|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| ⚙️ | **Architecture**         | cdf employs a Python-based, multi-workspace architecture, enhancing modularity and facilitating separation of concerns within data engineering projects. |
| 📄 | **Documentation**        | Comprehensive and user-friendly, aiding in easy setup, customization, and utilization of the framework. |
| 🔗 | **Dependencies**         | Built on Python, leveraging libraries like `dlt`, `sqlmesh`, `typer` for database operations and intuitive CLI interactions. |
| 🧩 | **Modularity**           | Features distinct workspaces, config files, and a clear separation between core functionalities and user-defined pipelines, models, and publishers. |
| 🧪 | **Testing**              | Robust testing framework ensuring reliability and stability of data operations. (wip) |
| ⚡️ | **Performance**          | Optimized for high performance, benefiting from efficient underlying libraries and minimal abstraction overhead. |
| 🔌 | **Integrations**         | Use dlt verified sources out of the box or roll your own sources. |
| 📶 | **Scalability**          | The framework's multi-workspace design and modular approach contribute to a high level of scalability, ideal for growing data demands. |
| 🚀 | **Ease of Use**          | User-centric design with a streamlined CLI and Python API, making it accessible for both beginners and experienced engineers. |
| 🌐 | **Collaboration**        | Facilitates collaborative development with support for multi-user environments and individual workspace configurations. |
| 🏷️ | **Versioning & Tagging** | Advanced version control and tagging for components, enabling effective management and tracking of data pipelines. |
| 📊 | **Data Management**      | Simplifies complex data pipeline creation, management, and execution, catering to continuous data integration and deployment. |

This table offers a snapshot of the `cdf` framework's features, emphasizing its robust, user-friendly, and scalable nature, making it an ideal choice for modern data engineering teams.


## 📂 Repository Structure

The following is the structure of the `cdf` repository workspace:

```sh
.
├── audits
├── macros
│   └── __init__.py
├── metadata
├── models
│   └── __init__.py
├── pipelines
│   └── __init__.py
├── publishers
│   └── __init__.py
├── scripts
│   └── __init__.py
├── seeds
├── tests
├── cdf_config.toml
└── requirements.txt

```

It extends the structure of a `sqlmesh` project with the following additions:

- `metadata`: A directory containing metadata files for the project.
- `cdf_config.toml`: A centralized templated configuration file for the project.
- `pipelines`: A directory containing pipeline definitions.
- `publishers`: A directory containing publisher definitions.
- `scripts`: A directory containing free-form flexible scripts for the project.

---

## ⚙️ Modules

<details open><summary>CDF Core</summary>

A collection of core modules for the CDF framework.

| File | Summary |
| --- | --- |
| [exception.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/exception.py) | This code defines a set of custom exceptions for the'cdf' package, each inheriting from a standard Python exception type. These include errors for type and attribute issues within a registry context, and file-related errors for when a source directory is not found or empty. |
| [config.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/config.py) | The code establishes a framework to manage configuration and secrets within a CDF (presumed to mean Custom Data Framework) project using TOML files. It defines custom config providers `CDFConfigTomlProvider` and `CDFSecretsTomlProvider` which search for a `cdf_config.toml` and a secrets file in the working directory or its parents up to 3 levels deep. Factories and helpers create, find, inject, or remove these providers in a global context. It also offers a function to populate function arguments using these configurations. The code integrates with an extensible `Workspace` object, facilitating workspace-specific configurations. |
| [constants.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/constants.py) | The code defines constants for CDF. It specifies key export symbols, names of core configuration files (workspace, config, secrets, and flags), a default workspace name, and paths for components within the CDF workspace structure. The `DIR_LAYOUT` tuple outlines the expected directory structure of a CDF workspace, including locations for pipelines, models, publishers, and other resources such as audits, macros, metadata, scripts, seeds, and tests. |
| [feature_flags.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/feature_flags.py) | The provided Python code defines a framework for managing feature flags within a Continuous Delivery Foundation (CDF) application. It includes abstract and concrete implementations of feature flag providers for local storage, Harness, and stubs for LaunchDarkly. Feature flags are used to toggle application features on and off without deploying new code.The core classes and functions:-`AbstractFeatureFlagProvider`: Defines abstract methods for checking, getting, creating, and dropping feature flags.-`LocalFeatureFlagProvider`: Implements the abstract class with a local file system-based storage mechanism.-`HarnessFeatureFlagProvider`: Implements the abstract class using the Harness feature flag service, including methods for interacting with the Harness API.-`process_source`: A function that processes and updates a data source's resources based on feature flags.-`get_provider`: Factory method returning an instance of the specific feature flag provider based on the given input.The code integrates with other CDF components, using caching and concurrency control (locks) for performance and safety. It also contains utility functions for translating between internal identifiers and external service identifiers. |
| [jinja.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/jinja.py) | The `jinja.py` file establishes a Jinja2 environment enabling'do' and'loopcontrols' extensions for additional templating features. It defines `JINJA_METHODS` with three functions: one to retrieve environment variables, another to format the current date as "YYYY-MM-DD," and a third for the previous day's date in the same format. These can be used within Jinja2 templates to inject dynamic content based on system environment settings and dates. |
| [logger.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/logger.py) | This code defines a customizable logging system for a package called "CDF." It features a `CDFLoggerAdapter` extending standard logging functionality and uses the `rich` library for enhanced console output. Users can configure the logger with a default or specified log level, retrieve the main logger or a named child logger, and set the log level dynamically. The logger is configured only once to avoid multiple configurations. The code includes dynamic attribute access to logger methods and type annotations for strict typing. |
| [utils.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/utils.py) | The `utils.py` module in the `cdf` package includes several utility functions for path augmentation, deep dictionary merging, module loading, function application, function representation, iterable flattening, JSON file searching and merging, and source component identification. It manipulates sys.path, applies supplied functions to iterable sequences, transforms functions to string descriptions, recursively flattens nested lists or tuples, searches for and combines JSON files up to a defined depth, and generates canonical identifiers for resources within a given source or workspace. |
| [transform.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/transform.py) | The script defines a data loading and transformation system for a SQL-based data framework. The `CDFTransformLoader` class extends `SqlMeshLoader` and focuses on processing YAML files containing model specifications to create SQL views. It utilizes file globs to locate YAML specs, constructs SQL projections with optional column prefix/suffix and computed columns, filters columns based on inclusion/exclusion patterns, and optionally applies predicates. Additionally, it handles metadata files for schema information and tracks source file paths. Models are registered with unique keys in a dictionary, supporting both SQL and external table models. |
| [monads.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/monads.py) | The code defines a generic `Monad` class and its two subclasses, `Option` and `Result`, all providing methods such as `map`, `flatmap`, `unwrap`, and various dunder methods (e.g., `__repr__`, `__bool__`, `__eq__`) in Python, while handling optional values (`None`) through the `Option` class and error-handling via the `Result` class, enhancing functional programming patterns within the context of the common data format (CDF) module. |
| [publisher.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/publisher.py) | The code defines a system for publishing data using customizable publishers with scheduled execution. The `Payload` class encapsulates data frames and their last execution timestamp. The `publisher_spec` dataclass describes a publisher with attributes for naming, the execution function (`runner`), source model, column mapping, version, owners, description, tags, cron scheduling, and enablement status. A publisher transforms column names for an external API and configures the runner with specific settings. The `export_publishers` function adds given publishers to the global scope, facilitating their retrieval and management in a centralized way. |
| [workspace.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/workspace.py) | The provided code defines two main classes, `Project` and `Workspace`, for managing projects and workspaces containing data pipelines, transformations, publications, and dependencies. `Project` deals with multiple workspaces, allowing adding or removing workspaces, and interacting with them collectively or individually. It can be created from a toml config, current directory, or dictionary. `Workspace` encapsulates a directory with its capabilities determined by the presence of specific subdirectories or files (pipelines, publishers, transforms, dependencies). It supports operations such as setting up a virtual environment, managing dependencies, reading/writing configs and lockfiles, and executing SQL transformations. Decorators enforce workspace capabilities for certain functions. Both classes provide utilities for loading configurations and components (pipelines, publishers, transforms) from the filesystem. The code is replete with context managers and decorators to manage environment state and ensure the proper setup of workspaces for execution. |
| [source.py](https://github.com/z3z1ma/cdf/blob/main/src/cdf/core/source.py) | The code defines a framework for building, running, and exporting data pipelines in a "Continuous Data Framework (CDF)." It allows the creation of pipelines that selectively process data resources with integrated metric capture. Pipelines are defined using `pipeline_spec`, which includes metadata and logic to run the pipeline with a specified sink and optional resources. Metrics are defined per resource and integrated into the pipeline using decorators. `export_pipelines` exports the defined pipelines to a global scope. The `SupportsPipelineClients` protocol ensures that pipeline clients support necessary methods. The code extensively uses Python type annotations for static type checking and readability. |

</details>

## 🚀 Getting Started

***Dependencies***

Please ensure you have the following dependencies installed on your system:

`- Python 3.10+`

`- duckdb 0.9+`

### 🔧 Installation

Install `cdf` with `pip`:
```sh
pip install cdf # concrete name on pypi pending
```

Or install `cdf` from source:


1. Clone the cdf repository:
```sh
git clone https://github.com/z3z1ma/cdf
```

2. Change to the project directory:
```sh
cd cdf
```

3. Install the package:
```sh
pip install -e .
```

### 🧪 Tests

Run the tests with `pytest`:

```sh
pytest tests
```

## 🛣 Project Roadmap

> - [ ] `ℹ️  Automated metadata capture into prod sink`


## 🤝 Contributing

Contributions are welcome! Here are several ways you can contribute:

- **[Submit Pull Requests](https://github.com/z3z1ma/cdf/blob/main/CONTRIBUTING.md)**: Review open PRs, and submit your own PRs.
- **[Join the Discussions](https://github.com/z3z1ma/cdf/discussions)**: Share your insights, provide feedback, or ask questions.
- **[Report Issues](https://github.com/z3z1ma/cdf/issues)**: Submit bugs found or log feature requests for z3z1ma.


#### *Contributing Guidelines*

<details closed>
<summary>Click to expand</summary>

1. **Fork the Repository**: Start by forking the project repository to your GitHub account.
2. **Clone Locally**: Clone the forked repository to your local machine using a Git client.
   ```sh
   git clone <your-forked-repo-url>
   ```
3. **Create a New Branch**: Always work on a new branch, giving it a descriptive name.
   ```sh
   git checkout -b new-feature-x
   ```
4. **Make Your Changes**: Develop and test your changes locally.
5. **Commit Your Changes**: Commit with a clear and concise message describing your updates.
   ```sh
   git commit -m 'Implemented new feature x.'
   ```
6. **Push to GitHub**: Push the changes to your forked repository.
   ```sh
   git push origin new-feature-x
   ```
7. **Submit a Pull Request**: Create a PR against the original project repository. Clearly describe the changes and their motivations.

Once your PR is reviewed and approved, it will be merged into the main branch.

</details>

---

## 📄 License


This project is distributed under the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0) License. For more details, refer to the [LICENSE](https://github.com/z3z1ma/cdf/blob/main/LICENSE) file.

---

## 👏 Acknowledgments

- Harness (https://harness.io/) for being the proving grounds in which the initial concept of this project was born.

[**Return**](#Top)

---


