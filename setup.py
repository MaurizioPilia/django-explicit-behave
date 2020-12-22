import os
from setuptools import find_packages, setup

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='django_urlqueryset',
    version=__import__('django_urlqueryset').get_version(),
    packages=find_packages(),
    include_package_data=True,
    url='https://github.com/MaurizioPilia/django-urlqueryset',
    install_requires=[
        'Django>=1.8',
        'djangorestframework>=3.9.4',
        'requests>=2.11.1',
    ],
    zip_safe=False,
)
