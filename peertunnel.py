import subprocess
import atexit
import os


class PeerTunnel:
    def __init__(self):
        self.proc: subprocess.Popen | None = None
        self.api_proc: subprocess.Popen = None

    def __call__(
        self,
        signal_room: str = 'defaultsignal',
        debug: bool = False,
    ) -> None:
        if not self.is_api_alive():
            self.install_deps()
            self.start_api()

        if not self.is_alive():
            self.start_tunnel(signal_room, debug)

        atexit.register(self.proc.terminate)

    def terminate(self) -> None:
        if self.proc:
            self.proc.terminate()
        if self.api_proc:
            self.api_proc.terminate()
        atexit.unregister(self.proc.terminate)

    def is_alive(self) -> bool:
        return self.proc and self.proc.poll() is None

    def is_api_alive(self) -> bool:
        return self.api_proc and self.api_proc.poll() is None

    def install_deps(sefl) -> None:
        pip = subprocess.Popen(
            f"pip install -r peer_requirements.txt",
            shell=True,
        )
        print("Installing peer dependencies...")
        pip.wait()

    def start_tunnel(self, signal_room: str, debug: bool) -> None:
        print("Starting PeerTunnel...")
        env = os.environ.copy()
        env['SIGNAL_ROOM'] = signal_room
        self.proc = subprocess.Popen(
            f"python datachannel_cf.py {'--debug' if debug else ''}",
            shell=True,
            env=env
        )

    def start_api(self) -> None:
        print("Starting api...")
        self.api_proc = subprocess.Popen(
            f"fastapi run signalapi.py",
            shell=True,
        )


peerTunnel = PeerTunnel()
