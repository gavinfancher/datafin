from setuptools import setup, find_packages

setup(
    name="datafin",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "pandas",
        "botocore"
    ],
    python_requires=">=3.12",
    zip_safe=False,
) 