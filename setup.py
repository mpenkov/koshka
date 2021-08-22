import os
import setuptools


def get_version():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(curr_dir, 'koshka', 'version.py')) as fin:
        line = fin.readline().strip()
        parts = line.split(' ')
        assert len(parts) == 3
        assert parts[0] == '__version__'
        assert parts[1] == '='
        return parts[2].strip('\'"')


with open('README.rst') as fin:
    long_description = fin.read()

setuptools.setup(
    author='Michael Penkov',
    author_email='m@penkov.dev',
    classifiers=[],
    description='Like GNU cat, but with autocompletion for S3.',
    long_description=long_description,
    entry_points={
        'console_scripts': {
            'kot=koshka.kot:main',
        }
    },
    install_requires=['argcomplete'],
    keywords=['cat'],
    name='koshka',
    packages=['koshka'],
    url='https://github.com/mpenkov/koshka',
    version=get_version(),
)
