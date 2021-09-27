import os

from explicit_behave import __version__
from setuptools import find_packages, setup

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='django-explicit-behave',
    version='.'.join(str(x) for x in __version__),
    description="A collection of explicit behave steps for API testing",
    author="Maurizio Pilia, Javier Buzzi",
    author_email="piliamaurizio@gmail.com, buzzi.javier@gmail.com",
    url="https://github.com/MaurizioPilia/django-explicit-behave",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'behave>=1.2.6',
        'Django>=1.11',
        'freezegun>=0.3.11',
        'PyYAML>=3.13',
        'jq>=0.1.6',
        'factory-boy>=2.12.0',
        'model-bakery>=1.3.2',
        'tabulate>=0.8.3',
    ],
    python_requires='>=3.6',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "Framework :: Django",
    ],
    zip_safe=False,
)
