try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='gstore',
    version='0.1',
    description='Geographic Storage, Transformation and Retrieval Engine',
    author='Renzo Sanchez-Silva',
    author_email='renzo@edac.unm.edu',
    url='http://gstore.unm.edu',
    install_requires=[
        "Pylons>=1.0",
        "SQLAlchemy>=0.5",
    ],
    setup_requires=["PasteScript>=1.6.3"],
    packages=find_packages(exclude=['ez_setup']),
    include_package_data=True,
    test_suite='nose.collector',
    package_data={'gstore': ['i18n/*/LC_MESSAGES/*.mo']},
    #message_extractors={'gstore': [
    #        ('**.py', 'python', None),
    #        ('templates/**.mako', 'mako', {'input_encoding': 'utf-8'}),
    #        ('public/**', 'ignore', None)]},
    zip_safe=False,
    paster_plugins=['PasteScript', 'Pylons'],
    entry_points="""
    [paste.app_factory]
    main = gstore.config.middleware:make_app

    [paste.paster_command]
    ingest-datasets = gstore.commands.ingest_datasets:IngestDatasets
    promote-vector-datasets = gstore.commands.ingest_datasets:PromoteVectorDatasets
    dump-vector-datasets = gstore.commands.ingest_datasets:DumpVectorDatasets    

    [paste.app_install]
    main = pylons.util:PylonsInstaller
    """,
)
