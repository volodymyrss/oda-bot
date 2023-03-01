from setuptools import setup, find_packages

install_req = [
    'oda-knowledge-base',
    'pyyaml',
    'click',
    'requests',
    'dynaconf',
    'rdflib',
    'nb2workflow',
    'oda_api',
    'mmoda_tab_generator'
]

test_req = [
]


setup(name='oda-bot',
      version="0.1.0",
      description='',
      author='V.S.',
      author_email='',
      packages=find_packages(),
      include_package_data=True,
      install_requires=install_req,
      extras_require={
          'test': test_req
      }
      )
