"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="kafka-reconciliation-results-verifier",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="A lambda that validates Athena query results produced for Kafka reconciliation",
    long_description="A lambda that validates Athena query results produced for Kafka reconciliation",
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": ["results_verifier_handler=results_verifier_handler:handler"]
    },
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=["argparse", "boto3", "moto"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
