"""Busca y descarga la edición de 2017 de Cigarettes After Sex con Nicotine+."""

import argparse
import re
import time
from collections import defaultdict
from pathlib import Path

from pynicotine.config import config
from pynicotine.core import core
from pynicotine.events import events
from pynicotine.slskmessages import FileListMessage
from pynicotine.transfers import TransferStatus


EXPECTED_DURATIONS = (319, 290, 214, 290, 274, 291, 364, 243, 258, 273)
TRACK_COUNT = len(EXPECTED_DURATIONS)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="búsqueda en Soulseek")
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--user")
    parser.add_argument("--folder")
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--sample-rate", type=int, default=44100)
    parser.add_argument("--bit-depth", type=int, choices=(16, 24), default=16)
    parser.add_argument("--stall-seconds", type=int, default=75)
    args = parser.parse_args()
    if min(args.seconds, args.sample_rate, args.stall_seconds) < 1:
        parser.error("los tiempos y la frecuencia deben ser positivos")
    if bool(args.user) != bool(args.folder) or bool(args.user) != bool(args.destination):
        parser.error("--user, --folder y --destination deben usarse juntos")

    found = defaultdict(dict)
    paths = defaultdict(dict)
    state = {"login": False, "responses": 0}

    def on_login(msg):
        if msg.success:
            state["login"] = True
            config.sections["searches"]["enable_history"] = False
            core.search.do_search(args.query, "global", switch_page=False)

    def on_response(msg):
        if not msg.list:
            return
        state["responses"] += 1
        for _, path, size, _, attrs in msg.list:
            if not path.lower().endswith(".flac"):
                continue
            folder, _, name = path.rpartition("\\")
            if "cigarettes after sex" not in folder.lower():
                continue
            _, duration, _, rate, depth = FileListMessage.parse_file_attributes(attrs)
            if duration is None or rate is None or depth is None:
                continue
            found[(msg.username, folder)][name] = (
                size, duration, rate, depth, msg.ulspeed, msg.inqueue, msg.freeulslots
            )
            paths[(msg.username, folder)][name] = (path, size, attrs)

    components = {
        "network_thread", "shares", "users", "network_filter", "search", "downloads", "uploads",
        "userbrowse", "pluginhandler", "statistics", "notifications", "buddies", "chatrooms",
        "privatechat", "userinfo", "interests"
    }
    core.init_components(enabled_components=components)
    original_plugins_enabled = config.sections["plugins"]["enable"]
    original_search_history = config.sections["searches"]["enable_history"]
    config.sections["plugins"]["enable"] = False
    events.connect("server-login", on_login)
    events.connect("file-search-response", on_response)
    try:
        core.start()
        core.connect()
        end = time.monotonic() + args.seconds
        while time.monotonic() < end and events.process_thread_events():
            time.sleep(0.1)

        if args.user:
            key = (args.user, args.folder)
            files = found.get(key, {})
            numbered = {}
            for name, data in files.items():
                match = re.match(r"^(\d{2})[. -]", name)
                if match:
                    number = int(match.group(1))
                    if number in numbered:
                        raise RuntimeError(f"Pista {number:02d} duplicada")
                    numbered[number] = (name, data)
            if set(numbered) != set(range(1, TRACK_COUNT + 1)):
                raise RuntimeError(f"Álbum incompleto: {len(numbered)} pistas numeradas")
            if any((data[2], data[3]) != (args.sample_rate, args.bit_depth) or
                   abs(data[1] - EXPECTED_DURATIONS[index - 1]) > 2
                   for index, (_, data) in numbered.items()):
                raise RuntimeError("Resolución o duración del candidato no coincide")
            if args.destination.exists():
                raise FileExistsError(f"Destino existente: {args.destination}")
            if not args.destination.parent.is_dir():
                raise FileNotFoundError(args.destination.parent)
            print(f"Fuente: {args.user}; {TRACK_COUNT} pistas; destino: {args.destination}", flush=True)
            for index in range(1, TRACK_COUNT + 1):
                name, _data = numbered[index]
                virtual_path, size, attrs = paths[key][name]
                core.downloads.enqueue_download(
                    args.user, virtual_path, folder_path=str(args.destination),
                    size=size, file_attributes=attrs
                )
                transfer = core.downloads.transfers[args.user + virtual_path]
                last_bytes = -1
                last_progress = time.monotonic()
                while events.process_thread_events():
                    if transfer.status == TransferStatus.FINISHED:
                        print(f"{index:02d}/{TRACK_COUNT} descargada: {name}", flush=True)
                        break
                    if transfer.current_byte_offset != last_bytes:
                        last_bytes = transfer.current_byte_offset
                        last_progress = time.monotonic()
                    if time.monotonic() - last_progress > args.stall_seconds:
                        core.downloads.abort_downloads([transfer])
                        raise TimeoutError(f"Fuente detenida en pista {index}: {transfer.status}")
                    time.sleep(0.2)
                else:
                    raise RuntimeError("Nicotine+ cerró la sesión")
    finally:
        config.sections["plugins"]["enable"] = original_plugins_enabled
        config.sections["searches"]["enable_history"] = original_search_history
        core.quit()
        for _ in range(30):
            if not events.process_thread_events():
                break
            time.sleep(0.1)

    if args.user:
        return
    print(f"Conexión: {state['login']}; respuestas: {state['responses']}; carpetas: {len(found)}")
    candidates = [(key, files) for key, files in found.items() if len(files) == TRACK_COUNT]
    candidates.sort(key=lambda item: (
        all(data[2] == args.sample_rate and data[3] == args.bit_depth
            for data in item[1].values()),
        next(iter(item[1].values()))[6],
        -next(iter(item[1].values()))[5],
        next(iter(item[1].values()))[4]
    ), reverse=True)
    for (user, folder), files in candidates[:15]:
        sample = next(iter(files.values()))
        spec = {(data[2], data[3]) for data in files.values()}
        print(f"{user} | {sum(data[0] for data in files.values()) // 1048576} MiB | "
              f"{spec} | {sample[4] // 1024} KiB/s | cola {sample[5]} | libre {sample[6]}")
        print(f"  {folder}")
        print("  " + " | ".join(f"{name}: {data[1]}s" for name, data in sorted(files.items())))


if __name__ == "__main__":
    main()
