import shutil
import subprocess
import atexit
from pathlib import Path

from urllib.request import urlopen
from tqdm.auto import tqdm

from pycloudflared.try_cloudflare import Urls, url_pattern, metrics_pattern
from pycloudflared.util import Info, get_info


def download(info: Info | None = None) -> str:
    if info is None:
        info = get_info()

    if info.system == "darwin" and info.machine == "arm64":
        print(
            "* On a MacOS system with an Apple Silicon chip, "
            "Rosetta 2 needs to be installed, "
            "refer to this guide to learn more: "
            "https://support.apple.com/en-us/HT211861"
        )  # noqa: E501

    dest = Path('/tmp') / info.url.split("/")[-1]

    with urlopen(info.url) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        with tqdm.wrapattr(
            resp, "read", total=total, desc="Download cloudflared..."
        ) as src:
            with dest.open("wb") as dst:
                shutil.copyfileobj(src, dst)

    if info.system == "darwin":
        # macOS file is a tgz archive
        shutil.unpack_archive(dest, dest.parent)
        dest.unlink()
        excutable = dest.parent / "cloudflared"
    else:
        excutable = dest
    excutable.chmod(0o777)

    return str(excutable)


async def patch_dns(api_url, key: str, target: str) -> None:
    import httpx
    fqdn = target.replace("https://", "")
    async with httpx.AsyncClient() as client:
        await client.patch(
            f'{api_url}',
            json={"content": fqdn},
            headers={"Authorization": f"Bearer {key}"}
        )
        print(f'DNS updated to {fqdn}')


class TryCloudflare:
    def __init__(self):
        self.running: dict[int, Urls] = {}

    def __call__(
        self,
        port: int | str,
        metrics_port: int | str | None = None,
        verbose: bool = False,
        update_dns: bool = False,
        secrets: dict = None,
    ) -> Urls:
        port = int(port)
        if port in self.running:
            urls = self.running[port]
            if verbose:
                self._print(urls.tunnel, urls.metrics)
            return urls

        self.running[port] = Urls('running', '', None)
        info = get_info()
        info.executable = Path('/tmp') / info.url.split("/")[-1]
        if not Path(info.executable).exists():
            info.executable = download(info)

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
        if update_dns:
            import asyncio
            asyncio.run(
                patch_dns(secrets['dns_api_url'], secrets['dns_api_key'], tunnel_url)
            )
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
