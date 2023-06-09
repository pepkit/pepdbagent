# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.

## [0.4.1] -- 2023-06-09
- Fixed project.update function

## [0.4.0] -- 2023-06-09
- Transitioned to SQLAlchemy ORM.
- Added a pep_schema column to the database.
- Implemented a new testing approach.
- Integrated sorting functionality into the annotations module.
- Temporarily disabled description-based searches to mitigate long processing times and reduce database load.
- Included timezone support in the database.
- Standardized namespace and names to lowercase for case-insensitivity.
- Streamlined the database schema creation process, with tables now created dynamically as needed.


## [0.3.1] -- 2023-03-23
- Fixed bug with peppy const dependencies


## [0.3.0] -- 2023-01-19
- Restructured pepdbagent: 
  - Renamed `Agent` class to `PEPDatabaseAgent`
  - created subclasses (project, annotation, namespace).
  - Merged methods that have similar use case.
  - Removed unused methods.
  - Simplified pepdbagent API
- Restructured database (added columns: `private`, `number_of_samples`, `submission_date`, `last_update_date`)
- Improved logging
- Added new tests
- Improved documentation


## [0.2.3] -- 2022-11-21

- Added more logging information in uploading project methods
- Added `update_only` argument `upload_project` method

## [0.2.2] -- 2022-11-09

- Fixed error in pepannotation (privacy error)

## [0.2.1] -- 2022-11-09

- Fixed error with namespace user if user is unknown

## [0.2.0] -- 2022-11-08

- Added limits and offset to function: `get_projects_in_namespace()`
- Annotation changed to BaseModel from pydantic
- Added privacy information in annotations
- Added update feature to `upload_project` method with `overwrite` argument
- Added mock tests
- Deleted unused functions:
  - `get_projects_in_list`
  - `get_projects_annotation_by_namespace`
  - `project_status`
  - `project_status_by_registry`
  - `get_registry_paths_by_digest`

- Changed method names:
  - `get_project_by_registry` --> `get_project_by_registry_path`
  - `get_project_annotation_by_registry` --> `get_project_annotation_by_registry_path`

## [0.1.0] -- 2022-08-18

- 🎉 first release!
