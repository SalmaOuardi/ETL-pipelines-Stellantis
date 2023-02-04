#!/usr/bin/sh
set -e
script_dirpath=$(dirname $0)
cd $script_dirpath/..
make install
source script/app_profile.sh

python "$UNXPACKAGE/pipeline/dp_imp_ap_data_pipeline.py"
