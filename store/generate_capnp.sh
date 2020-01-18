#!/usr/bin/env bash
set -ex
capnp compile -I$(go list -json -m zombiezen.com/go/capnproto2 | jq -r .Dir)/std -ogo segment.capnp
