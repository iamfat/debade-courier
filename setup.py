from setuptools import setup

setup(
    name='debade-courier',

    version='0.2.2',

    description='DeBaDe Courier',

    url='https://github.com/iamfat/debade-courier',

    author="Jia Huang",
    author_email="iamfat@gmail.com",

    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Utilities',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',
    ],

    keywords='debade courier',

    packages=['debade_courier'],

    install_requires=["pika", "pyyaml", "pyzmq"],

    entry_points={
        'console_scripts': [
            'debade-courier=debade_courier:main',
        ],
    },
)
