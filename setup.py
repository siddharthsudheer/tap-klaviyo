#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="tap-klaviyo",
    version="1.0.0",
    description="Singer.io tap for extracting Klaviyo data",
    author="Siddharth Sudheer",
    url="http://github.com/siddharthsudheer/tap-klaviyo",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_klaviyo"],
    install_requires=[
        "singer-python==5.4.1",
        "requests==2.20.0",
        "aiohttp==3.5.4",
        "asyncio==3.4.3"
    ],
    entry_points="""
    [console_scripts]
    tap-klaviyo=tap_klaviyo:main
    """,
    packages=find_packages(),
    package_data = {
        "schemas": ["tap_klaviyo/schemas/*.json"]
    },
    include_package_data=True
)
