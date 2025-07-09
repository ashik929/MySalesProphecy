from setuptools import setup, find_packages
setup(
    name = 'customeGEM',
    version = '1.0',
    packages = find_packages(include = ('customegem*', )) + ['prophecy_config_instances.customegem'],
    package_dir = {'prophecy_config_instances.customegem' : 'configs/resources/customegem'},
    package_data = {'prophecy_config_instances.customegem' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.49'],
    entry_points = {
'console_scripts' : [
'main = customegem.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
