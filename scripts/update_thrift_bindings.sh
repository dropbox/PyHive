#!/bin/bash -eux

HIVE_VERSION='2.1.1'  # Must be a released version

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
curl https://raw.githubusercontent.com/apache/hive/rel/release-$HIVE_VERSION/service-rpc/if/TCLIService.thrift > $tmpdir/TCLIService.thrift

# Apply patch
pushd $tmpdir
patch < TCLIService.patch
popd

thrift -r --gen py -out $scriptdir/../ $tmpdir/TCLIService.thrift
