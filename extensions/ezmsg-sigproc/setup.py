import setuptools

setuptools.setup(
    name="ezmsg-sigproc",
    packages=setuptools.find_namespace_packages(include=["ezmsg.*"]),
    zip_safe=False,
)
