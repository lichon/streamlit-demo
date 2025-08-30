import streamlit as st
import subprocess

from cloudflared import cloudflared
from streamlit_ttyd import get_ttyd
from streamlit.components.v1 import iframe

signalProc = None

def terminal(
    cmd: str = "echo terminal-speaking... && sleep 99999",
    writable: bool = True,
    port: int = 1234,
    exit_on_disconnect: bool = False,
    ttyd = ""
):
    assert type(port) == int

    flags = f"--port {port} "
    if exit_on_disconnect:
        flags += "--once "
    if writable:
        flags += "-W"

    # check if user provided path to ttyd
    ttyd = get_ttyd() if ttyd=="" else ttyd
    ttydproc = subprocess.Popen(
        f"{ttyd} {flags} {cmd}",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )

    return ttydproc, port

def signalRelay(
    debug: bool = False,
):
    global signalProc
    if signalProc:
        return
    signalProc = subprocess.Popen(
        f"python datachannel.py {'--debug' if debug else ''}",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )

signalRelay()

st.title("Streamlit Terminal")

# start the ttyd server
terminal(cmd="bash", port=1234)

tty_url = 'http://localhost:1234'
tty_url = cloudflared(1234).tunnel

iframe(tty_url, height=600)

# info on ttyd port
st.text(f"ttyd server is running at {tty_url}")
