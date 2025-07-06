from setuptools import setup, find_packages
setup(
    name = 'Data_ingestion',
    version = '1.0',
    packages = find_packages(include = ('data_ingestion*', )) + ['prophecy_config_instances.data_ingestion'],
    package_dir = {'prophecy_config_instances.data_ingestion' : 'configs/resources/data_ingestion'},
    package_data = {'prophecy_config_instances.data_ingestion' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.49'],
    entry_points = {
'console_scripts' : [
'main = data_ingestion.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
