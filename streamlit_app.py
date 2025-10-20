import streamlit as st
import subprocess
import os

from streamlit_wakeup import keepAlive
from peertunnel import peerTunnel
from cloudflared import cloudflared
from streamlit_ttyd import get_ttyd
from streamlit.components.v1 import iframe


def terminal(
    cmd: str = "echo terminal-speaking... && sleep 99999",
    auth: str = None,
    writable: bool = True,
    port: int = 1234,
    exit_on_disconnect: bool = False,
    ttyd: str = ""
):
    flags = f"--port {port} "
    if exit_on_disconnect:
        flags += "--once "
    if auth:
        flags += f"--credential {auth} "
    if writable:
        flags += "-W"

    # check if user provided path to ttyd
    ttyd = get_ttyd() if ttyd == "" else ttyd
    ttydproc = subprocess.Popen(
        f"{ttyd} {flags} {cmd}",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )
    return ttydproc, port


# start the peer tunnel
peerTunnel(secrets=st.secrets)

# start the ttyd server
terminal(cmd="bash", port=1234, auth=st.secrets["ttyd_auth"])

tty_url = 'http://localhost:1234'
peer_url = 'http://localhost:2234'

# streamlit server
if os.getenv("HOSTNAME") == "streamlit":
    # start playwright
    keepAlive(secrets=st.secrets)
    # set dns for cloudflared
    tty_url = cloudflared(1234).tunnel
    peer_url = cloudflared(2234, update_dns=True, secrets=st.secrets).tunnel


st.set_page_config(page_title="Streamlit Terminal", layout="wide")
# embed ttyd
iframe(tty_url, height=700)
# info on ttyd port
st.text(f"ttyd server is running at : {tty_url}")
st.text(f"peer server is running at : {peer_url}")
st.text(
    f"peer pid {peerTunnel.proc.pid} {'alive' if peerTunnel.is_alive() else 'dead'} "
    f"playwright pid {keepAlive.proc.pid} {'alive' if keepAlive.is_alive() else 'dead'}"
)
