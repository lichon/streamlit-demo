import subprocess
import atexit
import os

class PeerTunnel:
    def __init__(self):
        self.proc: subprocess.Popen | None = None

    def __call__(
        self,
        debug: bool = False,
        signal_room: str = 'defaultsignal'
    ) -> None:
        if self.is_alive():
            return
        pip = subprocess.Popen(
            f"pip install -r peer_requirements.txt",
            shell=True,
        )
        print("Installing peer dependencies...")
        pip.wait()
        env = os.environ.copy()
        env['SIGNAL_ROOM'] = signal_room
        self.proc = subprocess.Popen(
            f"python datachannel.py {'--debug' if debug else ''}",
            shell=True,
            env=env
        )
        print("Starting PeerTunnel...")
        atexit.register(self.proc.terminate)

    def terminate(self) -> None:
        if self.proc:
            self.proc.terminate()
            atexit.unregister(self.proc.terminate)

    def is_alive(self) -> bool:
        return self.proc and self.proc.poll() is None

peerTunnel = PeerTunnel()