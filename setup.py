from setuptools import setup, find_packages

import sys


if sys.version_info < (3, 7):
    sys.exit('Sorry, Python < 3.7 is not supported')


setup(
    name='siderpy',
    version='0.1',
    author='Roma Koshel',
    author_email='roma.koshel@gmail.com',
    license='MIT',
    py_modules=['siderpy'],
    install_requires=[
        'hiredis',
    ],
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    python_requires=">=3.7",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
    ],
)
