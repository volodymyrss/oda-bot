from setuptools import setup, find_packages

install_req = [
    'pyyaml',
    'click',
    'requests',
    'dynaconf',
    'sentry-sdk'
]

extras_req = {
    'test': [
        'pytest'
    ],
    'k8sdeploy': [
        'rdflib',
        'nb2workflow[k8s]',
        'oda_api',
        'cwltool',
        'mmoda_tab_generator',
        'markdown',
        'markdown-katex',
        'oda-knowledge-base',
    ],
    'galaxy': [
        'nb2workflow[galaxy]',
        'python-frontmatter',
    ]
    
}

setup(name='oda-bot',
      version="0.1.0",
      description='',
      author='V.S.',
      author_email='',
      packages=find_packages(),
      entry_points={'console_scripts': ['odabot=odabot.cli:main']},
      include_package_data=True,
      install_requires=install_req,
      extras_require=extras_req
      )
