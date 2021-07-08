#!/usr/bin/env python

from setuptools import setup

setup(name='tap-klaviyo',
      version='0.1.1',
      description='Singer.io tap for extracting data from the Klaviyo API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_klaviyo'],
      install_requires=[
          'singer-python==5.12.1',
          'requests==2.20.0',
          # 'pandas==1.2.4',
          # 'singer-tools==0.4.1'  # from 2017
          'git+git://github.com/singer-io/singer-tools@1654a5cfdd56b0b2fda49c344ff85c95fc7b0262'  # 04/05/2021
      ],
      entry_points='''
          [console_scripts]
          tap-klaviyo=tap_klaviyo:main
      ''',
      packages=['tap_klaviyo'],
      package_data={
          'tap_klaviyo/schemas': [
                "bounce.json",
                "click.json",
                "mark_as_spam.json",
                "open.json",
                "receive.json",
                "unsubscribe.json",
                "dropped_email.json",
                "global_exclusions.json",
                "lists.json",
                "subscribe_list.json",
                "unsub_list.json",
                "update_email_preferences.json",
          ]
      },
      include_package_data=True
)
