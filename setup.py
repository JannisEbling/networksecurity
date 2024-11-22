from typing import List

from setuptools import find_packages, setup


def get_requirements() -> List[str]:

    requirement_lst: List[str] = []
    try:
        with open("requirements.txt", "r") as file:

            lines = file.readlines()

            for line in lines:
                requirement = line.strip()
                if requirement and requirement != "-e .":
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found")

    return requirement_lst


setup(
    name="NetworkSecurity",
    version="0.0.1",
    author="Jannis Ebling",
    author_email="jannis.ebling@outlook.de",
    packages=find_packages(),
    install_requires=get_requirements(),
)
