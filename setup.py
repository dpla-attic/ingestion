#!/usr/bin/env python


# Generate akara.conf file by replacing tokens in akara.conf.template with
#  values in akara.ini. Tokens should take the form of: "${Section__Key}".
import ConfigParser
import string

ini = ConfigParser.ConfigParser()
ini.optionxform=str  # Maintain case for configuration keys
ini.read("akara.ini")

# Flatten sections and keys into a dictionary of the form {Section__Key: value, ...}
ini_tokens = dict()
for section in ini.sections():
    for token in ini._sections[section]:
        key = "%s__%s" % (section, token,)
        ini_tokens[key] = ini._sections[section][token]

# Generate akara.conf by replacing tokens in akara.conf.template
i = open("akara.conf.template", "r")
tpl = string.Template(i.read())
i.close()

out = open("akara.conf", "w")
out.write(tpl.safe_substitute(ini_tokens))
out.close()


# Standard setup.py follows
from distutils.core import setup

setup( name = 'ingestion',
       version = '33.10.2',
       description='DPLA Ingestion System',
       author='Digital Public Library of America',
       author_email='tech@dp.la',
       url='http://dp.la',
       package_dir={'dplaingestion':'lib'},
       packages=['dplaingestion','dplaingestion.akamod',
                 'dplaingestion.fetchers', 'dplaingestion.mappers'],
       scripts=['scripts/rollback_ingestion'],
)
