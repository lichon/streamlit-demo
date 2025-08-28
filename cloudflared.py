
import subprocess
import atexit
from pathlib import Path

from pycloudflared.try_cloudflare import Urls, url_pattern, metrics_pattern
from pycloudflared.util import get_info, download

class TryCloudflare:
    def __init__(self):
        self.running: dict[int, Urls] = {}

    def __call__(
        self,
        port: int | str,
        metrics_port: int | str | None = None,
        verbose: bool = True,
    ) -> Urls:
        info = get_info()
        if not Path(info.executable).exists():
            download(info)

        port = int(port)
        if port in self.running:
            urls = self.running[port]
            if verbose:
                self._print(urls.tunnel, urls.metrics)
            return urls

        args = [
            info.executable,
            "tunnel",
            "--url",
            f"http://127.0.0.1:{port}",
        ]

        if metrics_port is not None:
            args += [
                "--metrics",
                f"127.0.0.1:{metrics_port}",
            ]

        if info.system == "darwin" and info.machine == "arm64":
            args = ["arch", "-x86_64"] + args

        cloudflared = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )

        atexit.register(cloudflared.terminate)

        tunnel_url = metrics_url = ""

        lines = 20
        for _ in range(lines):
            line = cloudflared.stderr.readline()

            url_match = url_pattern.search(line)
            metric_match = metrics_pattern.search(line)
            if url_match:
                tunnel_url = url_match.group("url")
            if metric_match:
                metrics_url = "http://" + metric_match.group("url")

            if tunnel_url:
                break

        else:
            raise RuntimeError("Cloudflared failed to start")

        urls = Urls(tunnel_url, metrics_url, cloudflared)
        if verbose:
            self._print(urls.tunnel, urls.metrics)

        self.running[port] = urls
        return urls

    @staticmethod
    def _print(tunnel_url: str, metrics_url: str) -> None:
        print(f" * Running on {tunnel_url}")
        print(f" * Traffic stats available on {metrics_url}")

    def terminate(self, port: int | str) -> None:
        port = int(port)
        if port in self.running:
            self.running[port].process.terminate()
            atexit.unregister(self.running[port].process.terminate)
            del self.running[port]
        else:
            raise ValueError(f"port {port!r} is not running.")

cloudflared = TryCloudflare()