from setuptools import setup

setup(
    name='mesos_cook',
    version='0.1',
    description="Python client for Two Sigma's Cook scheduler",
    url='https://github.com/roguePanda/mesos_cook',
    author='Ben Navetta',
    author_email='benjamin_navetta@brown.edu',
    license='MIT',
    packages=['mesos_cook'],
    install_requires=['requests>=2.13.0', 'six>=1.10.0', 'fire>=0.1.0', 'pystachio>=0.8.3'],
    zip_safe=True
)
