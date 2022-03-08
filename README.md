# Sinko-pate

_Like syncopate, but from  pronounced /pɑˈteɪ/ (like pâté), from the Croatian 'to suffer'. 
To suffer from synch, or to make synching suffer..._

A mini testing framework for mongo synchronization tools.
Primarily used to test correctness cornercases and enable more rapid turn around of
issue discovery and resolution.

Components:

- SourceCluster
  - We ensure that balancer is stopped.
- DestinationCluster
  - Ensure the test collection is:
    - Present
    - has index for `shard_key`
    - is sharded by the shard key.
- LoadGenerators
  - create specific load/write paterns that are known to trip up synch suites
- DataSyncher
  - Orchestration wrapper to control the synch tool (in this case `mongopush`)
    - Start synch
    - Detect when sync is complete
    - Stop sync
- Validator
  - Validate that data is correctly synched from source to Destination
    - Uses `mongopush` `-verify` flag. 