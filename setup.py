from setuptools import setup, find_packages

setup(
    name="dd_mlops_costs",
    version="0.1.0",
    description="A cost tracking library for AWS Glue and EMR with Datadog integration for MLOps.",
    author="Tiago Lopes",
    author_email="tiago.lopes@datadoghq.com",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "datadog",
        "jmespath"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
