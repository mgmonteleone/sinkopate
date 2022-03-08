"""
1. Create target
2. Shard the target (collection)
3. DataSynch (indexes)
4. Quiecesse
5. Terminal Synch
6. Data Validation
7. Great victory!!!
"""
import os
import subprocess
import time
import pymongo.errors
from pymongo import MongoClient, ASCENDING
import logging
import threading
import queue
from enum import Enum

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
source_username = os.getenv('source_username', 'dba')
source_password = os.getenv('source_password', 'n33du2g0')
source_database = os.getenv('source_database', 'transactions_small')
source_full_host = os.getenv('source_host', 'adltest.l50ex.mongodb.net')

dest_full_host = os.getenv('dest_host', 'destination-test.l50ex.mongodb.net')
dest_username = os.getenv('dest_username', source_username)
dest_password = os.getenv('dest_password', source_password)
dest_database = os.getenv('dest_database', source_database)

collection_name = os.getenv('collection_name', 'transactions_small')

source_uri = os.getenv("source_uri", f"mongodb+srv://{source_username}:{source_password}"
                                     f"@{source_full_host}/{source_database}?"
                                     f"retryWrites=true&w=majority&readPreference=secondary"
                                     f"&readPreferenceTags=nodeType:ANALYTICS")

destination_uri = os.getenv("dest_uri", f"mongodb+srv://{dest_username}:{dest_password}"
                                        f"@{dest_full_host}/{dest_database}?"
                                        f"retryWrites=true&w=majority&readPreference=secondary")

mongopush_bin = 'bin/mongopush-osx-x64'
mongoverify_bin = 'bin/mongo_verify'


class BalancerStatuses(Enum):
    autoSplitOnly = "Autosplit only"
    off = "Off"
    full = "full"


class BalancerInfo:
    def __init__(self, source_dict: dict) -> None:
        self.mode: BalancerStatuses = BalancerStatuses(source_dict.get('mode'))
        self.in_balancer_round: bool = bool(source_dict.get('inBalancerRound', False))
        self.balancer_rounds: int = int(source_dict.get('numBalancerRounds', 0))


class SourceCluster:
    def __init__(self):
        self.client = MongoClient(source_uri)
        self.source_db = self.client[source_database]
        self.source_collection = self.client[source_database][collection_name]
        self.logger = logging.getLogger('SourceCluster')

    def is_balancer_running(self) -> BalancerInfo:
        try:
            output = self.client.admin.command({"balancerStatus": 1})
            self.logger.warning(f'Balancer status is: {BalancerInfo(output).__dict__}')
            return BalancerInfo(output)
        except Exception as e:
            raise e

    def disable_balancer(self):
        try:
            output = self.client.admin.command({"balancerStop": 1})
            self.logger.warning(f"Stopped the balancer {output}")
        except Exception as e:
            raise e

    def enable_balancer(self, timeout_ms: int = 60000):
        """

        :param timeout_ms: Time limit for enabling the balancer.
        """
        try:
            output = self.client.admin.command({"balancerStart": 1, "maxTimeMS": timeout_ms})
            self.logger.warning(f"Starting the balancer {output}")
        except Exception as e:
            raise e


class DestinationCluster:
    def __init__(self):
        self.client = MongoClient(destination_uri)
        self.destination_db = self.client[dest_database]
        self.destination_collection = self.client[dest_database][collection_name]
        self.logger = logging.getLogger('DestinationCluster')

    def is_collection_sharded(self) -> bool:
        try:
            output = self.destination_db.command({"collStats": collection_name})
            if output["sharded"]:
                self.logger.warning(f"The collection {collection_name} is sharded.")
            else:
                self.logger.warning(f"The collection {collection_name} is NOT sharded.")
            return output["sharded"]
        except pymongo.errors.OperationFailure:
            self.logger.info(f"The collection {collection_name}  does not exist on the destination cluster")
            return False

    def shard_the_collection(self, shard_key: list = ("fw", ASCENDING)):
        # First check to see if the collection is already sharded!
        shard_key_list = [list]
        if self.is_collection_sharded():
            self.logger.error(
                f"The collection {self.destination_collection.name} is already sharded! Will not shard it!!")
            raise pymongo.errors.InvalidOperation(f"The collection is already sharded! Will not shard it!!")
        else:
            self.logger.info(f"The collection {self.destination_collection.name} is not yet sharded")

        shard_key_index_exists = False
        shard_key_native = {}
        # Need to convert pymongo notation to the notation returned from coll info
        for each_tuple in shard_key_list:
            field = each_tuple[0]
            order = each_tuple[1]
            if order == ASCENDING:
                native_order = 1
            else:
                native_order = -1
            shard_key_native[field] = native_order

        # Ensure sharding is enabled on the dest database
        try:
            output = self.client.admin.command("enablesharding", dest_database)
        except Exception as e:
            raise e
        if output.get('ok') == 1:
            logger.info("Database has sharding enabled.")

        try:  # ensure that there is an index on the shard key
            output = self.destination_db.command({"listIndexes": collection_name})
            for each in output["cursor"]["firstBatch"]:
                if each['key'] == shard_key_native:
                    logger.info(
                        f"The index for shard key {shard_key} ({shard_key_native}) exists. Name = {each['name']}")
                    shard_key_index_exists = True

        except pymongo.errors.OperationFailure:
            logger.info(f"The collection {collection_name} does not exist on the destination collection, "
                        f"so the index can not exist!")
        # Creating the index
        if not shard_key_index_exists:
            index_output = self.destination_collection.create_index(shard_key)
            logger.warning(f"The index named {index_output} was created!")
        else:
            logger.info("The index exists, we will not create it!")

        # Shard the collection using the shard key.

        try:
            shard_output = self.client.admin.command("shardCollection", f"{dest_database}.{collection_name}",
                                                     key=shard_key_native)
            logger.info(f"The collection {self.destination_collection.name} was sharded..")
            logger.info(
                f"Return info: Collection NS= {shard_output.get('collectionsharded')}, "
                f"OK= {bool(shard_output.get('ok'))} collectionUUID: {shard_output.get('collectionUUID').as_uuid()}")
        except Exception as e:
            raise e

    def unshard_the_collection(self):
        if self.is_collection_sharded():
            logger.info(f"The destination collection ({self.destination_collection.name}) is sharded, will unshard it)")
        else:
            logger.warning(f"The destination collection ({self.destination_collection.name}) "
                           f"is not sharded, so do not need to un-shard it")
            pymongo.errors.CollectionInvalid("The destination collection is not sharded, do not need to un-shard it!")


class DataSyncher:
    def __init__(self, synch_binary: str = mongopush_bin, verify_binary: str = mongoverify_bin):
        self.synch_binary: str = synch_binary
        self.verify_binary: str = verify_binary
        self.logger = logging.getLogger('DataSyncher')

        self.source_cluster: SourceCluster = SourceCluster()
        self.destination_cluster: DestinationCluster = DestinationCluster()

        self.logger.warning(f"DataSyncher ready, Synch binary = {self.synch_binary}."
                            f" VerifyBinary = {self.verify_binary}")
        self.logger.warning(f"Source cluster: {self.source_cluster.__dict__}")
        self.logger.warning(f"Destination cluster: {self.destination_cluster.__dict__}")

        # Pre-flight checks
        self.logger.info(f"Preflight Checks starting. . .")
        if self.source_cluster.is_balancer_running() in [BalancerStatuses.full, BalancerStatuses.autoSplitOnly]:
            self.source_cluster.disable_balancer()
            output = self.source_cluster.is_balancer_running()
            self.logger.warning(
                f"The balancer was running on the source cluster, stopped it. Current status is {output.mode}")
        else:
            self.logger.info(f"The source cluster balancer was not running, no need to stop it.")
        # test reading from source cluster...
        try:
            test_output = self.source_cluster.source_collection.find_one({})
            self.logger.info(f"Connected successfully to source, Source record = {test_output.get('_id')}")
        except Exception as e:
            self.logger.error(F"Could not connect to source cluster, this synch will fail!")
            raise e

        self.logger.info(f"Preflight checks completed.")

    @staticmethod
    def output_reader(proc, outq):
        for line in iter(proc.stdout.readline, b''):
            outq.put(line.decode('utf-8'))

    def synch_start(self, timeout_secs: int = 120):
        timeout_ms: float = float(timeout_secs) * 1000
        start_time: float = time.time()
        timeout_time: float = start_time + timeout_ms
        logger.info(f'We will timeout this operation in {timeout_secs} seconds ({timeout_time})')

        process_options: list = [f"{mongopush_bin}", "-exec", "bin/config.json", "-license", "Apache-2.0"]
        proc = subprocess.Popen(process_options, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                stdin=subprocess.PIPE)

        outq = queue.Queue()
        t = threading.Thread(target=self.output_reader, args=(proc, outq))
        t.start()

        try:
            time.sleep(0.2)
            while time.time() < timeout_time:
                try:
                    line = outq.get(block=False)
                    print(f'{line}', end='')
                    if 'lag 0s' in line:
                        print("CAUGHT UP!!!")
                        proc.terminate()
                except queue.Empty:
                    pass

                time.sleep(0.1)

        finally:
            proc.terminate()
            try:
                proc.wait(timeout=0.2)
                print('== subprocess exited with rc =', proc.returncode)
            except subprocess.TimeoutExpired:
                print('subprocess did not terminate in time')

        t.join()


syncher = DataSyncher()
