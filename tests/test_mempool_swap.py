"""Run: .venv/bin/python tests/test_mempool_swap.py   (the mempool decoder reports a pending subnet-owner swap)"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.mempool import Mempool  # noqa: E402


class Meta:
    def __init__(self, ext):
        self.ext = ext

    async def decode_extrinsic(self, hx):
        return self.ext


def swap_call(new="5NewOwner"):
    return {"call_module": "SubtensorModule", "call_function": "swap_coldkey_announced",
            "call_args": [{"name": "new_coldkey", "value": new}]}


def run(ext):
    got = []
    mp = Mempool(None, Meta(ext), [], 1.0, 200, lambda n: None, lambda n: True, lambda p: None,
                 on_swap=lambda who, new, tx: got.append((who, new, tx)))
    asyncio.run(mp._analyze("0x" + "ab" * 40))
    return got


def test_a_pending_swap_is_reported_with_its_signer_and_the_new_coldkey():
    got = run({"address": "5Owner", "call": swap_call()})
    assert len(got) == 1 and got[0][:2] == ("5Owner", "5NewOwner") and got[0][2].startswith("0x")


def test_a_swap_sent_through_a_proxy_or_a_batch_names_the_owner_not_the_signer():
    proxy = {"call_module": "Proxy", "call_function": "proxy",
             "call_args": [{"name": "real", "value": "5Owner"}, {"name": "call", "value": swap_call()}]}
    assert run({"address": "5Delegate", "call": proxy})[0][:2] == ("5Owner", "5NewOwner")
    batch = {"call_module": "Utility", "call_function": "batch_all", "call_args": [{"name": "calls", "value": [swap_call()]}]}
    assert run({"address": "5Owner", "call": batch})[0][:2] == ("5Owner", "5NewOwner")


def test_other_calls_are_not_swaps():
    other = {"call_module": "SubtensorModule", "call_function": "start_call", "call_args": [{"name": "netuid", "value": 9}]}
    assert run({"address": "5Owner", "call": other}) == []


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} mempool swap tests passed")
