from setuptools import setup

try:
    import re
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
    long_description = re.sub("[.][.] figure.*?:alt:", "", long_description, flags=re.M+re.S)
except Exception as e:
    # pandoc or pypandoc is not installed, fallback to using raw contents
    with open('README.md') as f:
        long_description = f.read()

setup(
    name='parallelpipe',
    version='0.2.4',
    author='Giuseppe Tribulato',
    author_email='gtsystem@gmail.com',
    py_modules=['parallelpipe'],
    url='https://github.com/gtsystem/parallelpipe',
    license='MIT',
    description='Pipeline parallelization library',
    long_description=long_description,
)
