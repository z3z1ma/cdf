<div align="center">
<h1 align="center">
<img src="https://raw.githubusercontent.com/PKief/vscode-material-icon-theme/ec559a9f6bfd399b82bb44393651661b08aaf7ba/icons/database.svg" width="100" />
<br>CDF (Continuous Data Framework)</h1>
<h3>Craft end-to-end data pipelines and manage them continuously</h3>

<p align="center">
<img src="https://img.shields.io/badge/Python-3776AB.svg?style=flat-square&logo=Python&logoColor=white" alt="Python" />
<img src="https://img.shields.io/badge/sqlmesh-0.57.0+-blue" alt="SQLMesh" />
<img src="https://img.shields.io/badge/dlt-0.4.0+-blue" alt="dlt" />
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
- [🚀 Getting Started](#-getting-started)
- [📚 Documentation](#-documentation)
- [🤝 Contributing](#-contributing)
- [🛣 Roadmap](#-roadmap)
- [📄 License](#-license)
- [👏 Acknowledgments](#-acknowledgments)

---

## 📍 Overview

CDF (Continuous Data Framework) is an integrated framework designed to manage data across the entire lifecycle, from ingestion through transformation to publishing. It is built on top of two open-source projects, `sqlmesh` and `dlt`, providing a unified interface for complex data operations. CDF simplifies data engineering workflows, offering scalable solutions from small to large projects through an opinionated project structure that supports both multi-workspace and single-workspace layouts.

## Features

- **Unified Data Management**: Seamlessly manage data pipelines, transformations, and publishing within a single framework.
- **Opinionated Project Structure**: Adopt a scalable project structure that grows with your data needs, from single to multiple workspaces.
- **Automated Environment Management**: Automatically manage virtual environments to isolate and manage dependencies.
- **Automated Component Discoverability**: Automatically discover pipelines, models, publishers, and other components within your workspace.
- **Enhanced Configuration Management**: Leverage automated configuration management for streamlined setup and deployment.
- **Extensible and Scalable**: Designed to scale from small to large data projects, providing extensible components for custom operations.

## Getting Started

1. **Installation**:

    CDF requires Python 3.8 or newer. Install CDF using pip:

    ```bash
    pip install cdf
    ```

2. **Initialize a Workspace or Project**:

    Create a new workspace or project in your desired directory:

    ```bash
    cdf init-workspace /path/to/workspace
    # or
    cdf init-project /path/to/project
    ```

3. **Run Pipelines and Scripts**:

    Execute data pipelines, scripts, or notebooks within your workspace:

    ```bash
    cdf pipeline workspace_name.pipeline_name
    cdf execute-script workspace_name.script_name
    ```

4. **Publish Data**:

    Publish transformed data to external systems or sinks:

    ```bash
    cdf publish workspace_name.publisher_name
    ```

## Documentation

For detailed documentation, including API references and tutorials, visit [CDF Documentation](#).

## Contributing

Contributions to CDF are welcome! Please refer to the [contributing guidelines](CONTRIBUTING.md) for more information on how to submit pull requests, report issues, or suggest enhancements.

## License

CDF is licensed under [MIT License](LICENSE).

---

This README provides an overview of the CDF tool, highlighting its primary features, installation steps, basic usage examples, and contribution guidelines. It serves as a starting point for users to understand the capabilities of CDF and how it can be integrated into their data engineering workflows.
### 🧪 Tests

Run the tests with `pytest`:

```sh
pytest tests
```

## 🛣 Project Roadmap

TODO: Add a roadmap for the project.


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
7a. **Submit a Pull Request**: Create a PR against the original project repository. Clearly describe the changes and their motivations.

Once your PR is reviewed and approved, it will be merged into the main branch.

</details>

---

## 📄 License


This project is distributed under the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0) License. For more details, refer to the [LICENSE](https://github.com/z3z1ma/cdf/blob/main/LICENSE) file.

---

## 👏 Acknowledgments

- Harness (https://harness.io/) for being the proving grounds in which the initial concept of this project was born.
- SQLMesh (https://sqlmesh.com) for being a foundational pillar of this project as well as the team for their support,
advice, and guidance.
- DLT (https://dlthub.com) for being the other foundational pillar of this project as well as the team for their
support, advice, and guidance.

[**Return**](#Top)

---


