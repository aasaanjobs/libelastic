import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()

setup(
    name='libelastic',
    version='1.2',
    packages=['elastic_mapper'],
    description='A line of description',
    long_description=README,
    author='yourname',
    author_email='',
    url='https://github.com/aasaanjobs/libelastic',
    license='MIT',
    install_requires=[
        'Django>=1.9,<2.0',
        'elasticsearch>=6.2.0',
    ],
   python_requires='>=3.6'
)
