"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="emr-relauncher-handler",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="A lambda that deals with relaunching failed EMR clusters",
    long_description="A lambda that deals with relaunching failed EMR clusters",
    long_description_content_type="text/markdown",
    entry_points={"console_scripts": ["emr_relauncher_handler=emr_relauncher_handler:handler"]},
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=["argparse", "boto3", "moto"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
