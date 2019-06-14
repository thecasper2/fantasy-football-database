from setuptools import setup

setup(
	name='fantasy-database',
	version='0.1.0',
	packages=['fetch_data'],
	package_dir={'': 'src'},
	install_requires=[
		'sqlalchemy',
		'pandas',
		'tqdm',
		'fantasyprem'
	],
	dependency_links=[
		'git+git://github.com/thecasper2/fantasyfootballapi.git'
	],
	url='',
	license='',
	author='Alex Dolphin',
	author_email='',
	description=''
)
