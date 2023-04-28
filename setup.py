from setuptools import setup

setup(
    name='wavefront_analytics',
    version='0.2.4',
    url='https://github.com/keep94/wavefront_analytics',
    author='Travis Keep',
    author_email='travisk@vmware.com',
    py_modules=['wavefront_analytics'],
    install_requires=['wavefront_api_client>=2.176.0', 'urllib3>=1.26.15'],
)
