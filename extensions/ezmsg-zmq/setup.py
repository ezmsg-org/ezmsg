import setuptools

setuptools.setup(
    name="ezmsg-zmq",
    packages=setuptools.find_namespace_packages(include=["ezmsg.*"]),
    zip_safe=False,
)
