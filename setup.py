from setuptools import setup


setup(
    name="goblin",
    version="0.1.0",
    url="",
    license="MIT",
    author="davebshow",
    author_email="davebshow@gmail.com",
    description="Python driver for TP3 Gremlin Server",
    packages=["goblin", "goblin.gremlin_python",
              "goblin.gremlin_python_driver", "tests"],
    install_requires=[
        "aenum==1.4.5",
        "aiohttp==0.21.6",
        "inflection==0.3.1"
    ],
    test_suite="tests",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'
    ]
)
