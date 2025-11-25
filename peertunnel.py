import subprocess
import atexit
import os
import sys
import signal


class PeerTunnel:
    def __init__(self, pid_file: str = 'peertunnel.pid'):
        self.proc: subprocess.Popen | None = None
        self.pid_file: str = pid_file

    def __call__(
        self,
        secrets: dict = None,
    ) -> None:
        if not self.is_alive():
            self.start_tunnel(secrets)

        atexit.register(self.proc.terminate)

    def kill_old_pid(self) -> None:
        if not os.path.exists(self.pid_file):
            return
        with open(self.pid_file, 'r') as f:
            pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                print(f"Process {pid} not found")
            else:
                print(f"Killing old PeerTunnel process {pid}")
                os.kill(pid, signal.SIGTERM)

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
            [sys.executable, "datachannel/server.py", "--rtc"],
            shell=False,
            env=env
        )
        open(self.pid_file, 'w').write(str(self.proc.pid))


peerTunnel = PeerTunnel()
