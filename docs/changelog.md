# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.


## [0.1.0] -- 2022-07-19

### Changed

- Fetch a PEP by its registry path, is namespace and project name, its digest, or its id.
- All SQL queries now accept a `list` or a `tuple` for query parameterization instead of a series of unnamed args.
- If the `get_project` function returns `None` from the database, the internal `sql_run_fetchall/one` functions can now handle this and an error is raised and `None` is returned.
- Moved constants to `const.py` file.
- I renamed the invalid schema exception to be less verbose.
- Removed `conn.close()` from `try:` block since it is always closed in `finally:` block.

### Added

- New `get_namespaces` function with option for simple list of names or fully populated namespace information.
- New `get_projects` function that accepts a wide-range of inputs.
- New `get_namespace` function that will retreive information on a single namespace.
- Suite of tests for unit testing the package.
- New `utils.py` file.

### Fixed

- Resolved SQL injection vulnerability in `get_namespace`.
- 