#!/usr/bin/env bash

 SOURCE_URI="mongodb://${USER}:${PASSWORD}@${SOURCE}/${DATABASE}?authSource=admin&ssl=true"
 TARGET_URI="mongodb://${USER}:${PASSWORD}@${TARGET}/${DATABASE}?authSource=admin&ssl=true"

 namespace=${DATABASE}.${COLLECTION}
 JSON_FMT='{ "namespace": "%s" }'
 json_value=$(printf "$JSON_FMT" "$namespace")

 /mongopush -verify -workers $NUM_WORKERS -source "${SOURCE_URI}" -target "${TARGET_URI}" -include "${json_value}"