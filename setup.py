from setuptools import setup, find_packages


setup(name="namessync",
      version="0.1.0",
      python_requires=">=3.6",
      url="https://github.com/iobis/names-sync",
      license="MIT",
      author="Pieter Provoost",
      author_email="p.provoost@unesco.org",
      description="Non matching names sync",
      packages=find_packages(),
      zip_safe=False)
