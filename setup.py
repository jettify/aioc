import os
import re
import sys
from setuptools import setup, find_packages
from typing import List, Dict  # noqa


PY_VER = sys.version_info

if not PY_VER >= (3, 5):
    raise RuntimeError("aioc doesn't support Python earlier than 3.5")


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


install_requires = []  # type: List[str]
extras_require = {}  # type: Dict[str, str]


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'aioc', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            msg = 'Cannot find version in aioc/__init__.py'
            raise RuntimeError(msg)


classifiers = [
    'License :: OSI Approved :: MIT License',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Operating System :: POSIX',
    'Development Status :: 3 - Alpha',
]


setup(name='aioc',
      version=read_version(),
      description=('aioc'),
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.txt'))),
      classifiers=classifiers,
      platforms=['POSIX'],
      author="Nikolay Novik",
      author_email="nickolainovik@gmail.com",
      url='https://github.com/jettify/aioc',
      download_url='https://pypi.python.org/pypi/aioc',
      license='Apache 2',
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data=True)
