from setuptools import setup

install_req = [
]

test_req = [
]


setup(name='cdci_data_analysis',
      version=__version__,
      description='',
      author='V.S.',
      author_email='',
      packages=packs,
      include_package_data=True,
      install_requires=install_req,
      extras_require={
          'test': test_req
      }
      )
