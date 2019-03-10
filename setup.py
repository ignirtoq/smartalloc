import codecs
import os
import re
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]",
        version_file,
        re.M,
    )
    if version_match:
        return version_match.group(1)

    raise RuntimeError('Unable to find version string.')


NAME = 'smartalloc'
VERSION = find_version(NAME, '__init__.py')
DESCRIPTION = 'Resource allocation using an SMT solver.'
LONG_DESCRIPTION = read('README.md')
LONG_DESCRIPTION_CONTENT_TYPE = 'text/markdown',
LICENSE = 'MIT'
AUTHOR = 'Jeffrey Bouas'
EMAIL = 'ignirtoq+smartalloc@gmail.com'
URL = 'https://github.com/ignirtoq/smartalloc'
REQUIRES = ['z3-solver']
PACKAGES = find_packages()
CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
]


setup(
    name=NAME,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type=LONG_DESCRIPTION_CONTENT_TYPE,
    url=URL,
    author=AUTHOR,
    author_email=EMAIL,
    version=VERSION,
    license=LICENSE,
    install_requires=REQUIRES,
    packages=PACKAGES,
    classifiers=CLASSIFIERS,
)
