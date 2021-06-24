import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from string import printable

from electrum import SimpleConfig, Network
from electrum.util import create_and_start_event_loop, print_msg
from electrum.clients.client import ElectrumAsyncBatchClient, ElectrumThreadBatchClient


# region Create Network
config = SimpleConfig()
# config.set_key("server", "bitcoins.sk:50001:t")
config.set_key("server", "188.230.155.0:50001:t")

loop, stopping_fut, loop_thread = create_and_start_event_loop()

network = Network(config)
network.start()

while not network.is_connected():
    time.sleep(1)
    print_msg("waiting for network to get connected...")

# endregion


logging.basicConfig(format='%(asctime)-15s %(levelname)-8s %(message)s', level=logging.INFO)


BASE_DIR = Path(__file__).resolve().parent

# fp = BASE_DIR / "data"
shs_file = BASE_DIR / "shs.json"


# region create result files
mempools_res_file = BASE_DIR / "mempools_result.json"
unspents_res_file = BASE_DIR / "unspents_result.json"
balances_res_file = BASE_DIR / "balances_result.json"
# addresses = fp.open("r").readlines()

if not os.path.isfile(shs_file):
    open(shs_file, "w").close()

if not os.path.isfile(mempools_res_file):
    open(mempools_res_file, "w").close()

if not os.path.isfile(unspents_res_file):
    open(mempools_res_file, "w").close()

if not os.path.isfile(balances_res_file):
    open(mempools_res_file, "w").close()
# endregion


def delete_unread_symbols(string: str):
    return ''.join(char for char in string if char in printable)


shs = json.load(shs_file.open())

start_time = datetime.now()

ElectrumAsyncBatchClient()

print(f"START  ------ {start_time}")


with ElectrumAsyncBatchClient.get_instance() as client:
    unspents_reqs = list(map(lambda sh: client.get_listunspents(script_hash=sh), shs))
    mempools_reqs = list(map(lambda sh: client.get_listmempools(script_hash=sh), shs))
    balances_reqs = list(map(lambda sh: client.get_balances(script_hash=sh), shs))

# import ipdb; ipdb.set_trace()
mempools = {}
mempools_errors = {}
for req in mempools_reqs:
    r = req.result
    if isinstance(r, Exception):
        mempools_errors.update({req.params[0]: r})
    elif r is not None and len(r) > 0:
        mempools.update({req.params[0]: r})
json.dump(mempools, mempools_res_file.open(mode="w"))

unspents = {}
unspents_errors = {}
for req in unspents_reqs:
    r = req.result
    if isinstance(r, Exception):
        unspents_errors.update({req.params[0]: r})
    elif r is not None and len(r) > 0:
        unspents.update({req.params[0]: r})
json.dump(unspents, unspents_res_file.open(mode="w"))

balances = {}
balances_errors = {}
for req in balances_reqs:
    r = req.result
    if isinstance(r, Exception):
        balances_errors.update({req.params[0]: r})
    elif r is not None and len(r) > 0:
        balances.update({req.params[0]: r})
json.dump(balances, balances_res_file.open(mode="w"))

stop_time = datetime.now()
print(f"STOP  ------ {stop_time}")

print(mempools_errors)
print(unspents_errors)
print(balances_errors)

# region Output
print()
print("---------------------------")
print(f"start time: {start_time}")
print(f"stop time: {stop_time}")
print(f"requests count: {len(shs) * 3}")  # этот набор данных отправляется в 3 разных метода
print(f"response count: {len(mempools_reqs) + len(unspents_reqs) + len(balances_reqs)}")
print("---------------------------")
print()
# endregion

# ---------------------------
# start time: 2021-06-24 07:43:12.153571
# stop time: 2021-06-24 07:46:34.905086
# requests count: 620145
# response count: 620145
# ---------------------------


# region Stop network
stopping_fut.set_result(1)
while not stopping_fut.done():
    time.sleep(1)
loop_thread.join()
loop.stop()
loop.close()
# endregion
