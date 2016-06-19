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
    version='0.2.6',
    author='Giuseppe Tribulato',
    author_email='gtsystem@gmail.com',
    py_modules=['parallelpipe'],
    url='https://github.com/gtsystem/parallelpipe',
    license='MIT',
    description='Pipeline parallelization library',
    long_description=long_description,
    classifiers=(
        'Intended Audience :: Developers',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ),
)
