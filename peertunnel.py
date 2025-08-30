import subprocess
import atexit

class PeerTunnel:
    def __init__(self):
        self.proc: subprocess.Popen | None = None

    def __call__(
        self,
        debug: bool = False,
    ) -> None:
        if self.is_alive():
            return
        self.proc = subprocess.Popen(
            f"python datachannel.py {'--debug' if debug else ''}",
            shell=True,
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
