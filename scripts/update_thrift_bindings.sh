#!/bin/bash -eux

HIVE_RELEASE='release-2.3.0-rc1' # 'rel/rel/release-2.1.1'

# Create a temporary directory
scriptdir=`dirname $0`
tmpdir=$scriptdir/.thrift_gen

# Clean up previous generation attempts, in case it breaks things
if [ -d $tmpdir ]; then
  rm -rf $tmpdir
fi
mkdir $tmpdir

# Copy patch that adds legacy GetLog methods
cp $scriptdir/thrift-patches/TCLIService.patch $tmpdir

# Download TCLIService.thrift from Hive
curl https://raw.githubusercontent.com/apache/hive/$HIVE_RELEASE/service-rpc/if/TCLIService.thrift > $tmpdir/TCLIService.thrift

# Apply patch
pushd $tmpdir
patch < TCLIService.patch
popd

thrift -r --gen py -out $scriptdir/../ $tmpdir/TCLIService.thrift
