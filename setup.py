import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

requirement_files = [ f"requirements/{handler}.txt" for handler in [ "stratus" ] ]
install_requires = set()
for requirement_file in requirement_files:
    with open( requirement_file ) as f:
        for dep in f.read().split('\n'):
            if dep.strip() != '' and not dep.startswith('-e'):
                install_requires.add( dep )

setuptools.setup(
    name="stratus-endpoint",
    version="0.0.3",
    author="Thomas Maxwell",
    author_email="thomas.maxwell@nasa.gov",
    description="Base classes for creating service endpoints for the Stratus Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nasa-nccs-cds/stratus-endpoint",
    packages=setuptools.find_packages(),
#    install_requires=list(install_requires),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
