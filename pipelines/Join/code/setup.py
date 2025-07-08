from setuptools import setup, find_packages
setup(
    name = 'Join',
    version = '1.0',
    packages = find_packages(include = ('join*', )) + ['prophecy_config_instances.join'],
    package_dir = {'prophecy_config_instances.join' : 'configs/resources/join'},
    package_data = {'prophecy_config_instances.join' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.49'],
    entry_points = {
'console_scripts' : [
'main = join.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
