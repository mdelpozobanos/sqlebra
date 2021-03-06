import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="SQLebra", # Replace with your own username
    version="0.1",
    author="Marcos del Pozo Banos",
    author_email="mdelpozobanos@gmail.com",
    description="Agnostic SQL library wrapper supporting python data types",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mdelpozobanos/sqlebra",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "License :: Copyleft"
        "Development Status :: 3 - Alpha",
    ],
    python_requires='>=3.6',
)

# Compile: python setup.py bdist_wheel
# Install: python -m pip install dist/SQLebra-0.1-py3-none-any.whl
