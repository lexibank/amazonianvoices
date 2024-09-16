from setuptools import setup
import json


with open("metadata.json", encoding="utf-8") as fp:
    metadata = json.load(fp)

setup(
    name='lexibank_amazonianvoices',
    description=metadata['title'],
    license=metadata.get('license', ''),
    url=metadata.get('url', ''),
    py_modules=['lexibank_amazonianvoices'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'lexibank.dataset': [
            'amazonianvoices=lexibank_amazonianvoices:Dataset',
        ]
    },
    install_requires=[
        'pylexibank>=3.5.0',
        'cldfbench>=1.14.0',
        'cldfcatalog>=1.5.1'
        'zenodoclient>=0.5.0',
        'csvw>=3.1.3',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
