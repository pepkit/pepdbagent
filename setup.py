import os
import sys

from setuptools import find_packages, setup

PACKAGE_NAME = "pepdbagent"

# Ordinary dependencies
DEPENDENCIES = []
with open("requirements/requirements-all.txt", "r") as reqs_file:
    for line in reqs_file:
        if not line.strip():
            continue
        # DEPENDENCIES.append(line.split("=")[0].rstrip("<>"))
        DEPENDENCIES.append(line)

# Additional keyword arguments for setup().
extra = {"install_requires": DEPENDENCIES}


# Additional files to include with package
def get_static(name, condition=None):
    static = [
        os.path.join(name, f)
        for f in os.listdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), name))
    ]
    if condition is None:
        return static
    else:
        return [i for i in filter(lambda x: eval(condition), static)]


with open(f"{PACKAGE_NAME}/_version.py", "r") as versionfile:
    version = versionfile.readline().split()[-1].strip("\"'\n")

with open("README.md") as f:
    long_description = f.read()

setup(
    name=PACKAGE_NAME,
    packages=find_packages(),
    version=version,
    description="A python-based project metadata manager for portable encapsulated projects",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    keywords="project, metadata, bioinformatics, database",
    url="https://github.com/pepkit/pepdbagent/",
    author="Oleksandr Khoroshevskyi",
    license="BSD2",
    include_package_data=True,
    # tests_require=(["pytest"]),
    setup_requires=(["pytest-runner"] if {"test", "pytest", "ptr"} & set(sys.argv) else []),
    **extra,
)
