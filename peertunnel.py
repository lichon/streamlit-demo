import subprocess
import atexit
import os
import sys


class PeerTunnel:
    def __init__(self):
        self.proc: subprocess.Popen | None = None

    def __call__(
        self,
        secrets: dict = None,
    ) -> None:
        if not self.is_alive():
            self.start_tunnel(secrets)

        atexit.register(self.proc.terminate)

    def terminate(self) -> None:
        if self.proc:
            self.proc.terminate()
        atexit.unregister(self.proc.terminate)

    def is_alive(self) -> bool:
        return self.proc and self.proc.poll() is None

    def start_tunnel(self, secrets: dict) -> None:
        print("Starting PeerTunnel...")
        env = os.environ.copy()
        env['SIGNAL_ROOM'] = secrets["signal_room"]
        env['SUPABASE_URL'] = secrets["sb_url"]
        env['SUPABASE_KEY'] = secrets["sb_key"]
        env['ENDPOINT_DOMAIN'] = secrets["endpoint_domain"]
        self.proc = subprocess.Popen(
            [sys.executable, "datachannel_sb.py"],
            shell=False,
            env=env
        )


peerTunnel = PeerTunnel()
