from setuptools import setup, find_packages

with open("README.md") as readme_file:
    README = readme_file.read()

setup(
    name="taransay-hub",
    description="Taransay Data Forwarder",
    long_description=README,
    author="Sean Leavey",
    author_email="taransay@attackllama.com",
    url="https://github.com/SeanDS/taransay/",
    use_scm_version={"write_to": "taransayhub/_version.py"},
    packages=find_packages(),
    python_requires=">=3.7",
    setup_requires=["setuptools_scm"],
    install_requires=[
        "click == 7.1.2",
        "pyserial-asyncio == 0.4",
        "pyyaml == 5.3.1",
        "requests == 2.24.0",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-flake8",
            "faker",
            "black",
            "pre-commit",
            "pylint",
            "flake8",
            "flake8-bugbear",
        ]
    },
    entry_points={"console_scripts": ["taransayhub = taransayhub.__main__:hub"]},
    license="GPL-3.0-or-later",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
