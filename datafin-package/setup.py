from setuptools import setup, find_packages

setup(
    name="datafin",
    version="0.4.0",
    packages=find_packages(),
    package_data={
        "datafin": ["py.typed"],
    },
    install_requires=[
        "boto3",
        "pandas",
        "botocore",
        "requests",
        "pandas_market_calendars",
        "sqlalchemy"
    ],
    python_requires=">=3.12",
    zip_safe=False,
)