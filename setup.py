from setuptools import setup


setup(
    name="goblin",
    version="2.1.0rc2",
    url="",
    license="AGPL",
    author="davebshow",
    author_email="davebshow@gmail.com",
    description="Python toolkit for TP3 Gremlin Server",
    packages=["goblin", "goblin.driver"],
    install_requires=[
        "aiogremlin==3.2.6rc1",
        "inflection==0.3.1",
    ],
    test_suite="tests",
    setup_requires=['pytest-runner'],
    tests_require=['pytest-asyncio', 'pytest'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: OS Independent'
    ]
)
