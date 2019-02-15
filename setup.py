import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="stratus-endpoint",
    version="0.0.2",
    author="Thomas Maxwell",
    author_email="thomas.maxwell@nasa.gov",
    description="Base classes for creating service endpoints for the Stratus Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nasa-nccs-cds/stratus-endpoint",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
