from setuptools import setup, find_packages
from io import open
from os import path

import pathlib
# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# automatically captured required modules for install_requires in requirements.txt
with open(path.join(HERE, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if ('git+' not in x) and (
    not x.startswith('#')) and (not x.startswith('-'))]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs \
                    if 'git+' not in x]

setup (
    name = 'lightbeam',
    description = 'Sends JSONL data into an Ed-Fi API',
    version = '0.0.1',
    packages = find_packages(), # list of all packages
    package_data={'lightbeam': ['resources/*.txt']},
    install_requires = install_requires,
    python_requires='>=3',
    entry_points='''
        [console_scripts]
        lightbeam=lightbeam.__main__:main
    ''',
    zip_safe=False,
    author="Tom Reitz",
    keywords="data, transmission, api, edfi",
    long_description=README,
    long_description_content_type="text/markdown",
    license='Apache 2.0',
    url='https://github.com/edanalytics/lightbeam',
    download_url='https://github.com/edanalytics/lightbeam/archive/0.0.1.tar.gz',
    dependency_links=dependency_links,
    author_email='treitz@edanalytics.org',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Education",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Education",
        "Topic :: Office/Business",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities"
    ]
)