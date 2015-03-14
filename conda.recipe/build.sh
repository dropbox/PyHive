#!/bin/bash

$PYTHON setup.py install --single-version-externally-managed --record=record.txt
pushd ${SP_DIR}
wget https://s3.amazonaws.com/blaze_data/TCLIService.zip
unzip TCLIService.zip
wget https://raw.githubusercontent.com/cloudera/impyla/master/impala/thrift_sasl.py
echo `ls`
rm TCLIService.zip
popd

# Add more build steps here, if they are necessary.

# See
# http://docs.continuum.io/conda/build.html
# for a list of environment variables that are set during the build process.
