import setuptools

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
    version='0.1',
)
