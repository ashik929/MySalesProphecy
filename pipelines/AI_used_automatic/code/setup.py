from setuptools import setup, find_packages
setup(
    name = 'AI_used_automatic',
    version = '1.0',
    packages = find_packages(include = ('ai_used_automatic*', )) + ['prophecy_config_instances.ai_used_automatic'],
    package_dir = {'prophecy_config_instances.ai_used_automatic' : 'configs/resources/ai_used_automatic'},
    package_data = {'prophecy_config_instances.ai_used_automatic' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.49'],
    entry_points = {
'console_scripts' : [
'main = ai_used_automatic.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
