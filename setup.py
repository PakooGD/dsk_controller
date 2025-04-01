from setuptools import find_packages, setup

package_name = 'px4_controller'

setup(
    name=package_name,
    version='0.0.1',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='alexei',
    maintainer_email='johndoe@example.com',
    description='This package helps to subscribe to px4 topics and communicate with custom server',
    license='TODO: License declaration',
    entry_points={
        'console_scripts': [
            'px4_websocket_node = src.px4_websocket_node:main',
        ],
    },
)
