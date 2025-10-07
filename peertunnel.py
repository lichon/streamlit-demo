import subprocess
import atexit
import os


class PeerTunnel:
    def __init__(self):
        self.proc: subprocess.Popen | None = None

    def __call__(
        self,
        secrets: dict = None,
    ) -> None:
        if not self.is_alive():
            signal_room = secrets["signal_room"]
            sb_url = secrets["sb_url"]
            sb_key = secrets["sb_key"]
            self.start_tunnel(signal_room, sb_url, sb_key)

        atexit.register(self.proc.terminate)

    def terminate(self) -> None:
        if self.proc:
            self.proc.terminate()
        if self.api_proc:
            self.api_proc.terminate()
        atexit.unregister(self.proc.terminate)

    def is_alive(self) -> bool:
        return self.proc and self.proc.poll() is None

    def install_deps(sefl) -> None:
        pip = subprocess.Popen(
            f"pip install -r peer_requirements.txt",
            shell=True,
        )
        print("Installing peer dependencies...")
        pip.wait()

    def start_tunnel(self, signal_room: str, url: str, key: str) -> None:
        print("Starting PeerTunnel...")
        env = os.environ.copy()
        env['SIGNAL_ROOM'] = signal_room
        env['SUPABASE_URL'] = url
        env['SUPABASE_KEY'] = key
        self.proc = subprocess.Popen(
            f"python datachannel_sb.py",
            shell=True,
            env=env
        )


peerTunnel = PeerTunnel()
