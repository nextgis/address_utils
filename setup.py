import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()

requires = [
    'sqlalchemy',
    'psycopg2',
    ]

setup(name='address_utils',
      version='0.0',
      description='address_utils',
      long_description=README,
      classifiers=[
        "Programming Language :: Python",
        ],
      author='',
      author_email='',
      url='',
      keywords='DataBase Text Search',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=requires,
      test_suite="address_utils",
      )
