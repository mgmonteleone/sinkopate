export source=mongodb+srv://USERNAME:PASSWORD@THE-SOURCE-REPLICA-SET.[orgcode].mongodb.net/db?authSource=admin&readPreference=secondary&readPreferenceTags=nodeType:ANALYTICS
export target=mongodb+srv://USERNAME:PASSWORD@THE-SHARDED-CLUSTER.[orgcode].mongodb.net/db

mongopush -push index -source "${source}" -target "${target}"

# ^^ This deletes existing indexes, including the {user_uuid: 1} shard key index created by the sharding script. Get existing index stats. Does any index already exist with user_uuid as prefix ?

mongopush -push data -source "${source}" -target "${target}"