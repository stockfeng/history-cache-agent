import importlib.util
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest

MODULE = Path(__file__).resolve().parents[1] / 'docker' / 'probe-agent.py'
spec = importlib.util.spec_from_file_location('probe_agent', MODULE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ProbeTests(unittest.TestCase):
    def serve(self, response, policy=None):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'agent.sock')
            requests = []
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.bind(path)
                server.listen(1)
                server.settimeout(3)
                def worker():
                    client, _ = server.accept()
                    with client:
                        client.settimeout(3)
                        requests.append(client.recv(1024))
                        client.sendall(response)
                thread = threading.Thread(target=worker)
                thread.start()
                try:
                    if policy is None:
                        module.probe(path)
                    else:
                        module.suspension_policy(path, policy)
                finally:
                    thread.join(4)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(requests, [b'{"op":"ping"}\n' if policy is None else
                                                b'{"op":"suspension_policy_status"}\n'])

    def test_real_uds_pong_without_history_request(self):
        self.serve(b'{"status":"PONG"}\n')

    def test_wrong_truncated_and_oversized_response(self):
        for response in (b'{"status":"ERROR"}\n', b'{"status":"PONG"}', b'x' * 1025):
            with self.subTest(response=response[:30]), self.assertRaises(ValueError):
                self.serve(response)

    def test_policy_probe_verifies_loaded_hash_count_and_decoder(self):
        expected = dict(sha256='a' * 64, intervals=1)
        value = dict(status='OK', price_null_encoding='ddb-double-null-v1', **expected)
        self.serve((json.dumps(value) + '\n').encode(), expected)
        for change in ({'sha256': 'b' * 64}, {'intervals': 2}, {'status': 'ERROR'},
                       {'price_null_encoding': 'unknown'}, {'extra': True}):
            with self.assertRaises(ValueError):
                self.serve((json.dumps(dict(value, **change)) + '\n').encode(), expected)


if __name__ == '__main__':
    unittest.main()
