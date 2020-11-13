import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="livetrader",  # Replace with your own username
    version="0.1",
    author='lookis<Jingsi Liu>',
    author_email="lookisliu@gmail.com",
    description="A small and extendable framework for trading",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lookis/livetrader",
    packages=setuptools.find_packages(),
    install_requires=[
        'click',
        'tzlocal',
        'environs',
        'zerorpc',
        'pandas',
        'pymongo',
        'motor',
    ],
    python_requires='>=3.6',
)
