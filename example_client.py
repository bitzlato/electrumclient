import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from string import printable

from electrum import bitcoin, SimpleConfig, Network
from electrum.clients import ElectrumThreadBatchClient
from electrum.util import create_and_start_event_loop, print_msg


try:
    import configparser
except ImportError:
    import ConfigParser as configparser

logging.basicConfig(format='%(asctime)-15s %(levelname)-8s %(message)s', level=logging.INFO)


BASE_DIR = Path(__file__).resolve().parent

fp = BASE_DIR / "data"
mempools_res_file = BASE_DIR / "mempools_result.json"
unspents_res_file = BASE_DIR / "unspents_result.json"
balances_res_file = BASE_DIR / "balances_result.json"
addresses = fp.open("r").readlines()


config = SimpleConfig()
# config.set_key("server", "bitcoins.sk:50001:t")
config.set_key("server", "188.230.155.0:50001:t")

loop, stopping_fut, loop_thread = create_and_start_event_loop()

network = Network(config)
network.start()

while not network.is_connected():
    time.sleep(1)
    print_msg("waiting for network to get connected...")

if not os.path.isfile(mempools_res_file):
    open(mempools_res_file, "w").close()

if not os.path.isfile(unspents_res_file):
    open(mempools_res_file, "w").close()

if not os.path.isfile(balances_res_file):
    open(mempools_res_file, "w").close()


def delete_unread_symbols(string: str):
    return ''.join(char for char in string if char in printable)


if __name__ == '__main__':
    shs = list(map(lambda x: bitcoin.address_to_scripthash(delete_unread_symbols(x.strip())), addresses))

    print(f"START  ------ {datetime.now()}")
    with ElectrumThreadBatchClient(network, loop, stopping_fut, loop_thread, 50, thread_count=12) as client:
        mempools_req_ids = list(map(lambda sh: client.get_listmempools(script_hash=sh), shs))
        unspents_req_ids = list(map(lambda sh: client.get_listunspents(script_hash=sh), shs))
        balances_req_ids = list(map(lambda sh: client.get_balances(script_hash=sh), shs))

    mempools = {}
    for req_id in mempools_req_ids:
        mempools.update({client.results[req_id].params[0]: client.results[req_id].result})
    json.dump(mempools, mempools_res_file.open(mode="w"))

    unspents = {}
    for req_id in unspents_req_ids:
        unspents.update({client.results[req_id].params[0]: client.results[req_id].result})
    json.dump(unspents, unspents_res_file.open(mode="w"))

    balances = {}
    for req_id in balances_req_ids:
        balances.update({client.results[req_id].params[0]: client.results[req_id].result})
    json.dump(balances, balances_res_file.open(mode="w"))

    print(f"STOP  ------ {datetime.now()}")

    # 620145 requests
    # 15.37.27.1895  -- start
    # 15:40:13.1762  -- stop

