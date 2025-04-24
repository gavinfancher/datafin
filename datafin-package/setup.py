from setuptools import setup, find_packages

setup(
    name="datafin",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "json",
        "pandas",
        "botocore"
    ],
    python_requires=">=3.13",
    zip_safe=False,
) 