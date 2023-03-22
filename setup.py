import setuptools

setuptools.setup(
    install_requires=[
        'nest_asyncio',
        'requests',
        'boto3',
        'azure-storage-file-datalake',
        'azure-identity']
)
