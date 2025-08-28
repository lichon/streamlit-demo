import streamlit as st
import subprocess

from cloudflared import cloudflared
from streamlit_ttyd import get_ttyd
from streamlit.components.v1 import iframe

def terminal(
    cmd: str = "echo terminal-speaking... && sleep 99999",
    readonly: bool = False,
    host: str = "http://localhost",
    port: int = 0,
    exit_on_disconnect: bool = True,
    height: int = 400,
    ttyd = ""
):
    assert type(port) == int

    if port == 0:
        port = 1234

    flags = f"--port {port} "
    if exit_on_disconnect:
        flags += "--once "
    if readonly:
        flags += "--readonly"

    # check if user provided path to ttyd
    ttyd = get_ttyd() if ttyd=="" else ttyd
    ttydproc = subprocess.Popen(
        f"{ttyd} {flags} {cmd}",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )

    return ttydproc, port


st.text("Terminal with bash")

urls = cloudflared(1234)

# start the ttyd server
ttydprocess, port = terminal(cmd="-W bash", port=1234)

iframe(urls.tunnel, height=600)

# info on ttyd port
st.text(f"ttyd server is running {urls.tunnel} on port : {port}")
